"""
Google Drive Handler - Integration with Google Drive API
Handles authentication, file listing, and downloads
"""

import os
import io
import json
import re
import requests
from typing import List, Dict, Optional
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import tempfile


class GoogleDriveHandler:
    """Handle Google Drive operations"""

    def __init__(self, credentials_file=None, client_id=None, client_secret=None, credentials_token=None):
        """
        Initialize Google Drive handler

        Args:
            credentials_file: Path to service account JSON or uploaded file object (legacy)
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            credentials_token: OAuth2 credentials token dict (from previous auth)
        """
        self.service = None
        self.temp_dir = tempfile.gettempdir()
        self.credentials = None

        # OAuth2 authentication (preferred)
        if client_id and client_secret:
            if credentials_token:
                # Use existing token
                self._authenticate_oauth2_token(credentials_token)
            else:
                # Will need to complete OAuth flow
                self.client_id = client_id
                self.client_secret = client_secret
        # Legacy service account authentication
        elif credentials_file:
            self._authenticate(credentials_file)

    def _authenticate(self, credentials_file):
        """Authenticate with Google Drive API"""
        try:
            # Handle both file paths and uploaded file objects
            if hasattr(credentials_file, 'read'):
                # It's an uploaded file object
                creds_dict = json.loads(credentials_file.read())
            else:
                # It's a file path
                with open(credentials_file, 'r', encoding='utf-8') as f:
                    creds_dict = json.load(f)

            # Create credentials
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )

            # Build service
            self.service = build('drive', 'v3', credentials=credentials)
            print("✅ Google Drive authenticated successfully")

        except Exception as e:
            print(f"❌ Error authenticating with Google Drive: {e}")
            raise

    def _authenticate_oauth2_token(self, token_dict):
        """Authenticate using OAuth2 token"""
        try:
            credentials = Credentials.from_authorized_user_info(
                token_dict,
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            
            # Refresh if needed
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(None)
            
            self.credentials = credentials
            self.service = build('drive', 'v3', credentials=credentials)
            print("✅ Google Drive authenticated successfully (OAuth2)")
            
        except Exception as e:
            print(f"❌ Error authenticating with OAuth2 token: {e}")
            raise

    def create_oauth2_flow(self, redirect_uri='urn:ietf:wg:oauth:2.0:oob'):
        """Create OAuth2 flow for authorization"""
        client_config = {
            "web": {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [redirect_uri]
            }
        }
        
        flow = Flow.from_client_config(
            client_config,
            scopes=['https://www.googleapis.com/auth/drive.readonly'],
            redirect_uri=redirect_uri
        )
        
        return flow

    def get_authorization_url(self, redirect_uri='urn:ietf:wg:oauth:2.0:oob'):
        """Get authorization URL for OAuth2 flow"""
        flow = self.create_oauth2_flow(redirect_uri)
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent'
        )
        return authorization_url, state

    def complete_oauth2_flow(self, authorization_code, redirect_uri='urn:ietf:wg:oauth:2.0:oob'):
        """Complete OAuth2 flow with authorization code"""
        flow = self.create_oauth2_flow(redirect_uri)
        flow.fetch_token(code=authorization_code)
        
        # Get credentials
        self.credentials = flow.credentials
        self.service = build('drive', 'v3', credentials=self.credentials)
        
        # Return token dict for storage
        token_dict = {
            'token': self.credentials.token,
            'refresh_token': self.credentials.refresh_token,
            'token_uri': self.credentials.token_uri,
            'client_id': self.credentials.client_id,
            'client_secret': self.credentials.client_secret,
            'scopes': self.credentials.scopes
        }
        
        print("✅ Google Drive authenticated successfully (OAuth2)")
        return token_dict

    def extract_folder_id(self, folder_url: str) -> str:
        """
        Extract folder ID from Google Drive URL

        Args:
            folder_url: Full Google Drive folder URL

        Returns:
            Folder ID
        """
        # Handle different URL formats
        if '/folders/' in folder_url:
            return folder_url.split('/folders/')[-1].split('?')[0]
        elif 'id=' in folder_url:
            return folder_url.split('id=')[-1].split('&')[0]
        else:
            # Assume it's already just the ID
            return folder_url

    def list_folder_files_public(self, folder_url: str, file_types: Optional[List[str]] = None) -> List[Dict]:
        """
        List files in a PUBLIC Google Drive folder (no OAuth required)
        Uses improved parsing for public folders

        Args:
            folder_url: Google Drive folder URL or ID
            file_types: Optional list of MIME types to filter (by extension)

        Returns:
            List of file dictionaries with id, name
        """
        folder_id = self.extract_folder_id(folder_url)
        
        # Default to PDF
        if file_types is None:
            file_types = ['application/pdf']
        
        # Map MIME types to extensions
        ext_map = {
            'application/pdf': ['.pdf'],
            'application/vnd.google-apps.document': ['.gdoc', '.doc', '.docx'],
            'image/jpeg': ['.jpg', '.jpeg'],
            'image/png': ['.png']
        }
        
        allowed_extensions = []
        for mime in file_types:
            if mime in ext_map:
                allowed_extensions.extend(ext_map[mime])
        
        files = []
        
        try:
            # Try to access folder as public
            folder_view_url = f"https://drive.google.com/drive/folders/{folder_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(folder_view_url, timeout=15, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Cannot access folder. Status: {response.status_code}")
            
            # Parse HTML to extract file information
            html = response.text
            
            # Method 1: Look for embedded JSON data (Google Drive stores file info in JSON)
            # Try to find window['_DRIVE_ivd'] or similar JSON structures
            json_patterns = [
                r'window\["_DRIVE_ivd"\]\s*=\s*(\[.*?\]);',
                r'var\s+_DRIVE_ivd\s*=\s*(\[.*?\]);',
                r'\["([a-zA-Z0-9_-]+)"\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"',
            ]
            
            # Method 2: Look for file links in various formats
            file_link_patterns = [
                r'/file/d/([a-zA-Z0-9_-]+)',  # Standard file link
                r'id=([a-zA-Z0-9_-]+)',  # ID parameter
                r'folders/([a-zA-Z0-9_-]+)/view[^"]*id=([a-zA-Z0-9_-]+)',  # Folder view with file ID
            ]
            
            found_file_ids = set()
            
            # Try pattern 1: Extract from file links
            for pattern in file_link_patterns:
                matches = re.findall(pattern, html)
                if matches:
                    if isinstance(matches[0], tuple):
                        # If pattern returns tuple, take the file ID part
                        for match in matches:
                            if len(match) > 1:
                                found_file_ids.add(match[1])  # File ID is usually second
                            else:
                                found_file_ids.add(match[0])
                    else:
                        found_file_ids.update(matches)
            
            # Method 3: Look for data-item-id attributes
            data_item_pattern = r'data-item-id="([a-zA-Z0-9_-]+)"'
            data_items = re.findall(data_item_pattern, html)
            found_file_ids.update(data_items)
            
            # Method 4: Look for specific Google Drive data structures
            # Google Drive often embeds file info in script tags
            script_pattern = r'<script[^>]*>(.*?)</script>'
            scripts = re.findall(script_pattern, html, re.DOTALL)
            for script in scripts:
                # Look for file IDs in script content
                script_file_ids = re.findall(r'["\']([a-zA-Z0-9_-]{20,})["\']', script)
                found_file_ids.update(script_file_ids)
            
            print(f"🔍 Found {len(found_file_ids)} potential file IDs")
            
            # Now get file information for each ID
            for file_id in found_file_ids:
                # Skip if it's the folder ID itself
                if file_id == folder_id:
                    continue
                    
                try:
                    # Try to get file info via direct link
                    file_info_url = f"https://drive.google.com/file/d/{file_id}/view"
                    file_response = requests.get(file_info_url, timeout=5, allow_redirects=True, headers=headers)
                    
                    if file_response.status_code == 200:
                        # Extract filename from HTML
                        # Try multiple methods to get filename
                        filename = None
                        
                        # Method 1: From title tag
                        title_match = re.search(r'<title>([^<]+)</title>', file_response.text)
                        if title_match:
                            filename = title_match.group(1).replace(' - Google Drive', '').strip()
                        
                        # Method 2: From meta tags
                        if not filename:
                            meta_match = re.search(r'<meta\s+property="og:title"\s+content="([^"]+)"', file_response.text)
                            if meta_match:
                                filename = meta_match.group(1).strip()
                        
                        # Method 3: From JSON-LD
                        if not filename:
                            json_ld_match = re.search(r'"name"\s*:\s*"([^"]+)"', file_response.text)
                            if json_ld_match:
                                filename = json_ld_match.group(1).strip()
                        
                        # Fallback
                        if not filename:
                            filename = f"file_{file_id}"
                        
                        # Check if file extension matches
                        if any(filename.lower().endswith(ext) for ext in allowed_extensions):
                            files.append({
                                'id': file_id,
                                'name': filename,
                                'mimeType': 'application/pdf' if filename.lower().endswith('.pdf') else 'unknown',
                                'size': 0,  # Can't get size without API
                                'createdTime': None
                            })
                            print(f"  ✓ Found: {filename}")
                except Exception as e:
                    # Skip files that can't be accessed
                    continue
            
            if not files:
                # If no files found, try one more method: direct folder API call (works for some public folders)
                try:
                    # Try using the folder ID directly with export format
                    api_url = f"https://drive.google.com/drive/folders/{folder_id}?usp=sharing"
                    api_response = requests.get(api_url, timeout=10, headers=headers)
                    if api_response.status_code == 200:
                        # Look for any PDF references
                        pdf_refs = re.findall(r'([^/]+\.pdf)', api_response.text, re.IGNORECASE)
                        if pdf_refs:
                            print(f"⚠️ Found PDF references but couldn't extract file IDs. Consider using OAuth for better results.")
                except:
                    pass
            
            print(f"✅ Found {len(files)} files in public folder")
            return files
            
        except Exception as e:
            print(f"❌ Error accessing public folder: {e}")
            raise Exception(f"Folder is not publicly accessible or error occurred: {str(e)}")

    def list_folder_files(self, folder_url: str, file_types: Optional[List[str]] = None) -> List[Dict]:
        """
        List all files in a Google Drive folder
        Tries public access first, then OAuth if available

        Args:
            folder_url: Google Drive folder URL or ID
            file_types: Optional list of MIME types to filter

        Returns:
            List of file dictionaries with id, name, mimeType
        """
        # Try public access first (no OAuth needed)
        try:
            return self.list_folder_files_public(folder_url, file_types)
        except Exception as public_error:
            # If public access fails, try OAuth
            if not self.service:
                raise Exception(
                    f"Folder requires authentication. Public access failed: {str(public_error)}\n"
                    "Please authenticate with OAuth2 to access private folders."
                )
            
            # Use OAuth API
            folder_id = self.extract_folder_id(folder_url)

            # Default to PDF and common document types
            if file_types is None:
                file_types = [
                    'application/pdf',
                    'application/vnd.google-apps.document',
                    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                    'image/jpeg',
                    'image/png'
                ]

            files = []

            try:
                # Query for files in folder
                query = f"'{folder_id}' in parents and trashed=false"

                results = self.service.files().list(
                    q=query,
                    fields="files(id, name, mimeType, size, createdTime)",
                    pageSize=1000
                ).execute()

                items = results.get('files', [])

                # Filter by file type if specified
                for item in items:
                    if not file_types or item['mimeType'] in file_types:
                        files.append({
                            'id': item['id'],
                            'name': item['name'],
                            'mimeType': item['mimeType'],
                            'size': item.get('size', 0),
                            'createdTime': item.get('createdTime')
                        })

                print(f"✅ Found {len(files)} files in folder (OAuth)")
                return files

            except Exception as e:
                print(f"❌ Error listing folder files: {e}")
                raise

    def list_folder_recursive(self, folder_url: str, file_types: Optional[List[str]] = None) -> List[Dict]:
        """
        Recursively list all files in folder and subfolders

        Args:
            folder_url: Google Drive folder URL or ID
            file_types: Optional list of MIME types to filter

        Returns:
            List of all files in folder tree
        """
        if not self.service:
            raise Exception("Not authenticated with Google Drive")

        folder_id = self.extract_folder_id(folder_url)
        all_files = []

        def _recurse_folder(folder_id: str, path: str = ""):
            """Recursively get files from folder"""
            try:
                # Get all items in current folder
                query = f"'{folder_id}' in parents and trashed=false"
                results = self.service.files().list(
                    q=query,
                    fields="files(id, name, mimeType, size, createdTime)",
                    pageSize=1000
                ).execute()

                items = results.get('files', [])

                for item in items:
                    # If it's a folder, recurse into it
                    if item['mimeType'] == 'application/vnd.google-apps.folder':
                        subfolder_path = f"{path}/{item['name']}" if path else item['name']
                        _recurse_folder(item['id'], subfolder_path)
                    else:
                        # It's a file
                        if not file_types or item['mimeType'] in file_types:
                            file_info = {
                                'id': item['id'],
                                'name': item['name'],
                                'mimeType': item['mimeType'],
                                'size': item.get('size', 0),
                                'createdTime': item.get('createdTime'),
                                'path': f"{path}/{item['name']}" if path else item['name']
                            }
                            all_files.append(file_info)

            except Exception as e:
                print(f"Error processing folder {folder_id}: {e}")

        # Start recursion
        _recurse_folder(folder_id)

        print(f"✅ Found {len(all_files)} files in folder tree")
        return all_files

    def download_file_public(self, file_id: str, file_name: str) -> str:
        """
        Download a file from PUBLIC Google Drive (no OAuth required)
        
        Args:
            file_id: Google Drive file ID
            file_name: Name for the downloaded file
            
        Returns:
            Path to downloaded file
        """
        try:
            # Use direct download link for public files
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            
            # First request might redirect to a confirmation page for large files
            response = requests.get(download_url, stream=True, timeout=30)
            
            # Check if we got a virus scan warning page
            if 'virus scan warning' in response.text.lower() or 'download' in response.url.lower():
                # Extract the actual download link
                confirm_pattern = r'href="(/uc\?export=download[^"]+)"'
                match = re.search(confirm_pattern, response.text)
                if match:
                    download_url = "https://drive.google.com" + match.group(1)
                    response = requests.get(download_url, stream=True, timeout=30)
            
            # Download to temp directory
            file_path = os.path.join(self.temp_dir, file_name)
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            print(f"✅ Downloaded (public): {file_name}")
            return file_path
            
        except Exception as e:
            print(f"❌ Error downloading public file {file_name}: {e}")
            raise

    def download_file(self, file_id: str, file_name: str) -> str:
        """
        Download a file from Google Drive
        Tries public download first, then OAuth if available

        Args:
            file_id: Google Drive file ID
            file_name: Name for the downloaded file

        Returns:
            Path to downloaded file
        """
        # Try public download first
        try:
            return self.download_file_public(file_id, file_name)
        except Exception as public_error:
            # If public download fails, try OAuth
            if not self.service:
                raise Exception(
                    f"File requires authentication. Public download failed: {str(public_error)}\n"
                    "Please authenticate with OAuth2 to access private files."
                )
            
            try:
                # Get file metadata
                file_metadata = self.service.files().get(fileId=file_id).execute()
                mime_type = file_metadata['mimeType']

                # Handle Google Docs (need to export)
                if mime_type.startswith('application/vnd.google-apps'):
                    if 'document' in mime_type:
                        # Export as PDF
                        request = self.service.files().export_media(
                            fileId=file_id,
                            mimeType='application/pdf'
                        )
                        file_name = file_name.replace('.gdoc', '.pdf')
                    else:
                        raise Exception(f"Unsupported Google Apps type: {mime_type}")
                else:
                    # Regular file download
                    request = self.service.files().get_media(fileId=file_id)

                # Download to temp directory
                file_path = os.path.join(self.temp_dir, file_name)

                with io.FileIO(file_path, 'wb') as fh:
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while not done:
                        _, done = downloader.next_chunk()

                print(f"✅ Downloaded (OAuth): {file_name}")
                return file_path

            except Exception as e:
                print(f"❌ Error downloading file {file_name}: {e}")
                raise

    def download_folder(self, folder_url: str, recursive: bool = True) -> List[Dict]:
        """
        Download entire folder from Google Drive

        Args:
            folder_url: Google Drive folder URL or ID
            recursive: Whether to include subfolders

        Returns:
            List of downloaded file information
        """
        # Get file list
        if recursive:
            files = self.list_folder_recursive(folder_url)
        else:
            files = self.list_folder_files(folder_url)

        downloaded_files = []

        # Download each file
        for file_info in files:
            try:
                file_path = self.download_file(file_info['id'], file_info['name'])
                downloaded_files.append({
                    **file_info,
                    'local_path': file_path
                })
            except Exception as e:
                print(f"Failed to download {file_info['name']}: {e}")

        return downloaded_files
