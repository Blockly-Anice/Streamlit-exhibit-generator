"""
Streamlit Visa Exhibit Generator
Standalone application for generating numbered exhibit packages from PDFs, folders, and Google Drive
With built-in PDF compression

EXHIBIT ORGANIZATION REFERENCE:
This application follows the authoritative exhibit organization guide:
../VISA_EXHIBIT_RAG_COMPREHENSIVE_INSTRUCTIONS.md

This guide provides:
- Complete exhibit ordering templates for O-1A, O-1B, P-1A, P-1S, EB-1A
- Standard vs Comparable Evidence structures (O-1A, O-1B, EB-1A only)
- Criterion-by-criterion organization
- USCIS regulatory compliance (8 CFR § 214.2, 8 CFR § 204.5)
- Production-tested petition structures
"""

from gc import disable
import streamlit as st
import os
import tempfile
from pathlib import Path
from typing import List, Dict, Optional
import zipfile
from datetime import datetime
import requests
from urllib.parse import urlparse, parse_qs

# Import our modules
from pdf_handler import PDFHandler
from exhibit_processor import ExhibitProcessor
from google_drive import GoogleDriveHandler
from archive_handler import ArchiveHandler

# Check if compression is available
try:
    from compress_handler import USCISPDFCompressor, compress_pdf_batch
    COMPRESSION_AVAILABLE = True
except ImportError:
    COMPRESSION_AVAILABLE = False

# Page config
st.set_page_config(
    page_title="Visa Exhibit Generator",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .feature-box {
        padding: 1.5rem;
        border-radius: 0.5rem;
        background-color: #f0f2f6;
        margin: 1rem 0;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
    }
    .warning-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
    }
    .stat-card {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: white;
        border: 1px solid #ddd;
        text-align: center;
    }
    .stat-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .stat-label {
        font-size: 0.9rem;
        color: #666;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'exhibits_generated' not in st.session_state:
    st.session_state.exhibits_generated = False
if 'compression_stats' not in st.session_state:
    st.session_state.compression_stats = None
if 'exhibit_list' not in st.session_state:
    st.session_state.exhibit_list = []
if 'drive_files_loaded' not in st.session_state:
    st.session_state.drive_files_loaded = []
if 'drive_authenticated' not in st.session_state:
    st.session_state.drive_authenticated = False
if 'drive_credentials' not in st.session_state:
    st.session_state.drive_credentials = None
if 'drive_client_id' not in st.session_state:
    st.session_state.drive_client_id = ""
if 'drive_client_secret' not in st.session_state:
    st.session_state.drive_client_secret = ""
if 'oauth_auth_url' not in st.session_state:
    st.session_state.oauth_auth_url = None
if 'url_list' not in st.session_state:
    st.session_state.url_list = []
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = 0
if 'exhibit_page' not in st.session_state:
    st.session_state.exhibit_page = 1
if 'current_numbering_style' not in st.session_state:
    st.session_state.current_numbering_style = "letters"
if 'show_results_message' not in st.session_state:
    st.session_state.show_results_message = False
if 'force_results_tab' not in st.session_state:
    st.session_state.force_results_tab = False
if 'upload_files_page' not in st.session_state:
    st.session_state.upload_files_page = 1
if "selected_tab" not in st.session_state:
    st.session_state.selected_tab = "📁 Upload Files"

def extract_drive_download_url(drive_url: str) -> str:
    """
    Extract direct PDF download URL from Google Drive share link using HTML scraping.
    Works with /file/d/.../view, /preview, /open?id=... formats.
    No OAuth, no API, just simple HTTP + HTML parsing.
    
    Args:
        drive_url: Google Drive share link (file, preview, or open link)
        
    Returns:
        Direct PDF download URL (uc?export=download format)
    """
    import re
    
    try:
        # Extract file ID from various Google Drive URL formats
        file_id = None
        
        # Format 1: /file/d/FILE_ID/view
        if '/file/d/' in drive_url:
            file_id = drive_url.split('/file/d/')[1].split('/')[0].split('?')[0]
        # Format 2: /open?id=FILE_ID
        elif '/open?id=' in drive_url:
            file_id = drive_url.split('/open?id=')[1].split('&')[0]
        # Format 3: /preview?id=FILE_ID
        elif '/preview?id=' in drive_url:
            file_id = drive_url.split('/preview?id=')[1].split('&')[0]
        # Format 4: id=FILE_ID (in query params)
        elif 'id=' in drive_url:
            file_id = drive_url.split('id=')[1].split('&')[0].split('#')[0]
        
        if not file_id:
            raise Exception("Could not extract file ID from URL")
        
        # Fetch the page HTML without opening browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Try different URL formats to get the HTML
        view_url = f"https://drive.google.com/file/d/{file_id}/view"
        response = requests.get(view_url, headers=headers, timeout=15, allow_redirects=True)
        
        if response.status_code != 200:
            # Fallback: try with /open format
            open_url = f"https://drive.google.com/open?id={file_id}"
            response = requests.get(open_url, headers=headers, timeout=15, allow_redirects=True)
        
        if response.status_code != 200:
            raise Exception(f"Could not access Google Drive page. Status: {response.status_code}")
        
        html = response.text
        
        # Method 1: Search for "downloadUrl" in HTML (Google Drive embeds this in JSON)
        # Look for patterns like: "downloadUrl":"https://..."
        download_url_patterns = [
            r'"downloadUrl"\s*:\s*"([^"]+)"',
            r'"downloadUrl":\s*"([^"]+)"',
            r'downloadUrl["\']?\s*[:=]\s*["\']([^"\']+)["\']',
        ]
        
        download_url = None
        for pattern in download_url_patterns:
            matches = re.findall(pattern, html)
            if matches:
                download_url = matches[0]
                # Clean up escape sequences
                download_url = download_url.replace('\\u003d', '=').replace('\\/', '/')
                break
        
        # Method 2: If downloadUrl not found, construct direct download URL
        if not download_url:
            # Use the standard Google Drive direct download format
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        else:
            # Ensure it's a valid URL (sometimes Google returns relative URLs)
            if not download_url.startswith('http'):
                if download_url.startswith('//'):
                    download_url = 'https:' + download_url
                elif download_url.startswith('/'):
                    download_url = 'https://drive.google.com' + download_url
                else:
                    # Fallback to standard format
                    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        
        return download_url
        
    except Exception as e:
        # Fallback: return standard direct download format
        try:
            # Try to extract file ID one more time
            if '/file/d/' in drive_url:
                file_id = drive_url.split('/file/d/')[1].split('/')[0].split('?')[0]
            elif 'id=' in drive_url:
                file_id = drive_url.split('id=')[1].split('&')[0].split('#')[0]
            else:
                raise Exception("Could not extract file ID")
            
            return f"https://drive.google.com/uc?export=download&id={file_id}"
        except:
            raise Exception(f"Error extracting download URL: {str(e)}")

def download_pdf_from_url(url: str, output_path: str) -> bool:
    """
    Download a PDF file from a URL
    
    Args:
        url: URL of the PDF file
        output_path: Path where to save the downloaded file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Handle Google Drive file URLs - extract direct download URL
        if 'drive.google.com' in url:
            url = extract_drive_download_url(url)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, stream=True, timeout=30, headers=headers, allow_redirects=True)
        
        # Check if it's a PDF
        content_type = response.headers.get('Content-Type', '').lower()
        if 'pdf' not in content_type and not url.lower().endswith('.pdf'):
            # Check first few bytes for PDF magic number
            first_bytes = response.content[:4]
            if first_bytes != b'%PDF':
                return False
        
        # Handle Google Drive virus scan warning
        if 'virus scan warning' in response.text.lower() or 'download' in response.url.lower():
            # Extract the actual download link
            import re
            confirm_pattern = r'href="(/uc\?export=download[^"]+)"'
            match = re.search(confirm_pattern, response.text)
            if match:
                url = "https://drive.google.com" + match.group(1)
                response = requests.get(url, stream=True, timeout=30, headers=headers)
        
        # Download the file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        # Verify it's a valid PDF
        if os.path.getsize(output_path) > 0:
            with open(output_path, 'rb') as f:
                if f.read(4) == b'%PDF':
                    return True
        
        return False
        
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def get_filename_from_url(url: str) -> str:
    """Extract filename from URL"""
    try:
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        
        # If no filename in URL, try to get from Content-Disposition header
        if not filename or filename == '/':
            # For Google Drive, use file ID as name
            if 'drive.google.com' in url and '/file/d/' in url:
                file_id = url.split('/file/d/')[1].split('/')[0].split('?')[0]
                return f"drive_file_{file_id}.pdf"
            return "downloaded_file.pdf"
        
        # Ensure .pdf extension
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
        
        return filename
    except:
        return "downloaded_file.pdf"

def extract_pdf_urls_from_drive_folder(folder_url: str, use_oauth: bool = False, 
                                       client_id: str = None, client_secret: str = None, 
                                       credentials_token: dict = None) -> List[str]:
    """
    Extract PDF file URLs from a Google Drive folder
    
    Args:
        folder_url: Google Drive folder URL
        use_oauth: Whether to use OAuth authentication
        client_id: OAuth client ID (if using OAuth)
        client_secret: OAuth client secret (if using OAuth)
        credentials_token: OAuth credentials token (if using OAuth)
        
    Returns:
        List of direct download URLs for PDF files
    """
    try:
        # Initialize Google Drive handler
        if use_oauth and client_id and client_secret and credentials_token:
            drive_handler = GoogleDriveHandler(
                client_id=client_id,
                client_secret=client_secret,
                credentials_token=credentials_token
            )
            # Use OAuth API
            files = drive_handler.list_folder_files(folder_url, file_types=['application/pdf'])
        else:
            # Try public access
            drive_handler = GoogleDriveHandler()
            files = drive_handler.list_folder_files_public(folder_url, file_types=['application/pdf'])
        
        # Convert file IDs to direct download URLs using HTML scraping
        pdf_urls = []
        for file_info in files:
            file_id = file_info['id']
            # Use HTML scraping to get direct download URL (no OAuth, no API)
            file_view_url = f"https://drive.google.com/file/d/{file_id}/view"
            download_url = extract_drive_download_url(file_view_url)
            pdf_urls.append(download_url)
        
        return pdf_urls
        
    except Exception as e:
        raise Exception(f"Error extracting PDF URLs from folder: {str(e)}")

def generate_exhibits_from_urls(
    urls: List[str],
    visa_type: str,
    numbering_style: str,
    enable_compression: bool,
    quality_preset: str,
    smallpdf_api_key: Optional[str],
    add_toc: bool,
    add_archive: bool,
    merge_pdfs: bool
):
    """Generate exhibit package from PDF URLs"""
    with st.spinner("🔄 Processing PDFs from URLs..."):
        try:
            # Create PDF handler with compression settings
            pdf_handler = PDFHandler(
                enable_compression=enable_compression,
                quality_preset=quality_preset,
                smallpdf_api_key=smallpdf_api_key
            )

            # Create temporary directory for processing
            with tempfile.TemporaryDirectory() as tmp_dir:
                file_paths = []
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Download files from URLs
                status_text.text("📥 Downloading PDFs from URLs...")
                for i, url in enumerate(urls):
                    try:
                        filename = get_filename_from_url(url)
                        file_path = os.path.join(tmp_dir, filename)
                        
                        status_text.text(f"📥 Downloading {i+1}/{len(urls)}: {filename}")
                        
                        if download_pdf_from_url(url, file_path):
                            file_paths.append(file_path)
                            progress_bar.progress((i + 1) / len(urls))
                        else:
                            st.warning(f"⚠️ Failed to download: {url}")
                    except Exception as e:
                        st.warning(f"⚠️ Error downloading {url}: {str(e)}")
                        continue

                if not file_paths:
                    st.error("❌ No PDFs were successfully downloaded from the URLs")
                    return

                status_text.text(f"✓ Downloaded {len(file_paths)} file(s)")

                # Compression phase
                compression_results = []
                total_original_size = 0
                total_compressed_size = 0

                if enable_compression:
                    status_text.text("🗜️ Compressing PDFs...")
                    
                    for i, file_path in enumerate(file_paths):
                        if not os.path.exists(file_path):
                            st.warning(f"⚠️ File not found, skipping: {file_path}")
                            continue
                            
                        result = pdf_handler.compressor.compress(file_path) if pdf_handler.compressor else {'success': False}

                        if result['success']:
                            compression_results.append(result)
                            total_original_size += result['original_size']
                            total_compressed_size += result['compressed_size']

                            status_text.text(
                                f"🗜️ Compressed {i+1}/{len(file_paths)}: "
                                f"{result['reduction_percent']:.1f}% reduction ({result['method']})"
                            )

                        progress_bar.progress((i + 1) / len(file_paths))

                    # Calculate average compression
                    if compression_results:
                        avg_reduction = (1 - total_compressed_size / total_original_size) * 100 if total_original_size > 0 else 0

                        st.session_state.compression_stats = {
                            'original_size': total_original_size,
                            'compressed_size': total_compressed_size,
                            'avg_reduction': avg_reduction,
                            'method': compression_results[0]['method'] if compression_results else 'none',
                            'quality': quality_preset
                        }

                        status_text.text(f"✓ Compression complete: {avg_reduction:.1f}% average reduction")

                # Number exhibits
                status_text.text("📝 Numbering exhibits...")

                exhibit_list = []
                numbered_files = []

                for i, file_path in enumerate(file_paths):
                    # Get exhibit number
                    if numbering_style == "letters":
                        exhibit_num = chr(65 + i)  # A, B, C...
                    elif numbering_style == "numbers":
                        exhibit_num = str(i + 1)  # 1, 2, 3...
                    else:  # roman
                        exhibit_num = to_roman(i + 1)  # I, II, III...

                    # Add exhibit number to PDF
                    numbered_file = pdf_handler.add_exhibit_number(file_path, exhibit_num)
                    numbered_files.append(numbered_file)

                    # Track exhibit info
                    exhibit_info = {
                        'number': exhibit_num,
                        'title': Path(file_path).stem,
                        'filename': os.path.basename(file_path),
                        'pages': get_pdf_page_count(file_path)
                    }

                    # Add compression info if available
                    if i < len(compression_results) and compression_results[i]['success']:
                        exhibit_info['compression'] = {
                            'reduction': compression_results[i]['reduction_percent'],
                            'method': compression_results[i]['method']
                        }

                    exhibit_list.append(exhibit_info)

                    progress_bar.progress((i + 1) / len(file_paths))
                    status_text.text(f"📝 Numbered exhibit {exhibit_num}")

                st.session_state.exhibit_list = exhibit_list

                # Generate TOC if requested
                if add_toc:
                    status_text.text("📋 Generating Table of Contents...")
                    toc_file = pdf_handler.generate_table_of_contents(
                        exhibit_list,
                        visa_type,
                        os.path.join(tmp_dir, "TOC.pdf")
                    )
                    numbered_files.insert(0, toc_file)

                # Merge PDFs if requested
                if merge_pdfs:
                    status_text.text("📦 Merging PDFs...")
                    
                    output_file = os.path.join(tmp_dir, "final_package.pdf")
                    merged_file = pdf_handler.merge_pdfs(numbered_files, output_file)

                    # Verify the merged file was created
                    if os.path.exists(merged_file):
                        # Save to session state for download (outside temp directory)
                        final_output = os.path.join(tempfile.gettempdir(), f"exhibit_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
                        import shutil
                        shutil.copy(merged_file, final_output)
                        
                        # Verify copy was successful
                        if os.path.exists(final_output):
                            st.session_state.output_file = final_output
                            status_text.text(f"✓ PDF saved: {os.path.basename(final_output)}")
                        else:
                            st.error("❌ Failed to save output file")
                    else:
                        st.error(f"❌ Merged PDF not found at: {merged_file}")
                else:
                    st.info("ℹ️ PDFs processed individually (merge disabled)")

                progress_bar.progress(100)
                status_text.text("✓ Generation complete!")

                st.session_state.exhibits_generated = True
                st.session_state.active_tab = 2  # Mark to navigate to Results tab
                st.session_state.show_results_message = True  # Show navigation message
                
                # Show success message with auto-navigation
                st.success("🎉 **Generation Complete!** Automatically navigating to Results tab...")
                
                st.rerun()

        except Exception as e:
            st.error(f"❌ Error generating exhibits from URLs: {str(e)}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())

def generate_exhibits_from_drive(
    drive_files,
    visa_type: str,
    numbering_style: str,
    enable_compression: bool,
    quality_preset: str,
    smallpdf_api_key: Optional[str],
    add_toc: bool,
    add_archive: bool,
    merge_pdfs: bool,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    credentials_token: Optional[dict] = None
):
    """Generate exhibit package from Google Drive files"""
    with st.spinner("🔄 Processing files from Google Drive..."):
        try:
            # Initialize Google Drive handler (with or without OAuth)
            if client_id and client_secret and credentials_token:
                drive_handler = GoogleDriveHandler(
                    client_id=client_id,
                    client_secret=client_secret,
                    credentials_token=credentials_token
                )
            else:
                # Use public access
                drive_handler = GoogleDriveHandler()

            # Create PDF handler with compression settings
            pdf_handler = PDFHandler(
                enable_compression=enable_compression,
                quality_preset=quality_preset,
                smallpdf_api_key=smallpdf_api_key
            )

            # Create temporary directory for processing
            with tempfile.TemporaryDirectory() as tmp_dir:
                file_paths = []
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Download files from Google Drive
                status_text.text("📥 Downloading files from Google Drive...")
                for i, file_info in enumerate(drive_files):
                    try:
                        # Download file (will try public first, then OAuth if available)
                        file_path = drive_handler.download_file(
                            file_info['id'],
                            file_info['name']
                        )
                        # Move to our temp directory
                        new_path = os.path.join(tmp_dir, os.path.basename(file_path))
                        import shutil
                        if os.path.dirname(file_path) != tmp_dir:
                            shutil.move(file_path, new_path)
                        else:
                            new_path = file_path
                        file_paths.append(new_path)
                        progress_bar.progress((i + 1) / len(drive_files))
                        status_text.text(f"📥 Downloaded {i+1}/{len(drive_files)}: {file_info['name']}")
                    except Exception as e:
                        st.warning(f"⚠️ Failed to download {file_info['name']}: {str(e)}")
                        continue

                if not file_paths:
                    st.error("❌ No files were successfully downloaded")
                    return

                status_text.text(f"✓ Downloaded {len(file_paths)} file(s)")

                # Compression phase
                compression_results = []
                total_original_size = 0
                total_compressed_size = 0

                if enable_compression:
                    status_text.text("🗜️ Compressing PDFs...")
                    
                    for i, file_path in enumerate(file_paths):
                        if not os.path.exists(file_path):
                            st.warning(f"⚠️ File not found, skipping: {file_path}")
                            continue
                            
                        result = pdf_handler.compressor.compress(file_path) if pdf_handler.compressor else {'success': False}

                        if result['success']:
                            compression_results.append(result)
                            total_original_size += result['original_size']
                            total_compressed_size += result['compressed_size']

                            status_text.text(
                                f"🗜️ Compressed {i+1}/{len(file_paths)}: "
                                f"{result['reduction_percent']:.1f}% reduction ({result['method']})"
                            )

                        progress_bar.progress((i + 1) / len(file_paths))

                    # Calculate average compression
                    if compression_results:
                        avg_reduction = (1 - total_compressed_size / total_original_size) * 100 if total_original_size > 0 else 0

                        st.session_state.compression_stats = {
                            'original_size': total_original_size,
                            'compressed_size': total_compressed_size,
                            'avg_reduction': avg_reduction,
                            'method': compression_results[0]['method'] if compression_results else 'none',
                            'quality': quality_preset
                        }

                        status_text.text(f"✓ Compression complete: {avg_reduction:.1f}% average reduction")

                # Number exhibits
                status_text.text("📝 Numbering exhibits...")

                exhibit_list = []
                numbered_files = []

                for i, file_path in enumerate(file_paths):
                    # Get exhibit number
                    if numbering_style == "letters":
                        exhibit_num = chr(65 + i)  # A, B, C...
                    elif numbering_style == "numbers":
                        exhibit_num = str(i + 1)  # 1, 2, 3...
                    else:  # roman
                        exhibit_num = to_roman(i + 1)  # I, II, III...

                    # Add exhibit number to PDF
                    numbered_file = pdf_handler.add_exhibit_number(file_path, exhibit_num)
                    numbered_files.append(numbered_file)

                    # Track exhibit info
                    exhibit_info = {
                        'number': exhibit_num,
                        'title': Path(file_path).stem,
                        'filename': os.path.basename(file_path),
                        'pages': get_pdf_page_count(file_path)
                    }

                    # Add compression info if available
                    if i < len(compression_results) and compression_results[i]['success']:
                        exhibit_info['compression'] = {
                            'reduction': compression_results[i]['reduction_percent'],
                            'method': compression_results[i]['method']
                        }

                    exhibit_list.append(exhibit_info)

                    progress_bar.progress((i + 1) / len(file_paths))
                    status_text.text(f"📝 Numbered exhibit {exhibit_num}")

                st.session_state.exhibit_list = exhibit_list

                # Generate TOC if requested
                if add_toc:
                    status_text.text("📋 Generating Table of Contents...")
                    toc_file = pdf_handler.generate_table_of_contents(
                        exhibit_list,
                        visa_type,
                        os.path.join(tmp_dir, "TOC.pdf")
                    )
                    numbered_files.insert(0, toc_file)

                # Merge PDFs if requested
                if merge_pdfs:
                    status_text.text("📦 Merging PDFs...")
                    
                    output_file = os.path.join(tmp_dir, "final_package.pdf")
                    merged_file = pdf_handler.merge_pdfs(numbered_files, output_file)

                    # Verify the merged file was created
                    if os.path.exists(merged_file):
                        # Save to session state for download (outside temp directory)
                        final_output = os.path.join(tempfile.gettempdir(), f"exhibit_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
                        import shutil
                        shutil.copy(merged_file, final_output)
                        
                        # Verify copy was successful
                        if os.path.exists(final_output):
                            st.session_state.output_file = final_output
                            status_text.text(f"✓ PDF saved: {os.path.basename(final_output)}")
                        else:
                            st.error("❌ Failed to save output file")
                    else:
                        st.error(f"❌ Merged PDF not found at: {merged_file}")
                else:
                    st.info("ℹ️ PDFs processed individually (merge disabled)")

                progress_bar.progress(100)
                status_text.text("✓ Generation complete!")

                st.session_state.exhibits_generated = True
                st.session_state.active_tab = 2  # Mark to navigate to Results tab
                st.session_state.show_results_message = True  # Show navigation message
                
                # Show success message with auto-navigation
                st.success("🎉 **Generation Complete!** Automatically navigating to Results tab...")                
                
                st.rerun()

        except Exception as e:
            st.error(f"❌ Error generating exhibits from Google Drive: {str(e)}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())

def main():
    """Main application"""

    # Header
    st.markdown('<div class="main-header">📄 Visa Exhibit Generator</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Professional numbered exhibit packages for visa petitions</div>', unsafe_allow_html=True)

    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        # Visa type selection
        visa_type = st.selectbox(
            "Visa Type",
            ["O-1A", "O-1B", "P-1A", "EB-1A"],
            help="Select the visa category for your petition"
        )

        # Exhibit numbering style
        numbering_style = st.selectbox(
            "Exhibit Numbering",
            ["Letters (A, B, C...)", "Numbers (1, 2, 3...)", "Roman (I, II, III...)"],
            help="How to number your exhibits"
        )

        # Convert numbering style to code
        numbering_map = {
            "Letters (A, B, C...)": "letters",
            "Numbers (1, 2, 3...)": "numbers",
            "Roman (I, II, III...)": "roman"
        }
        numbering_code = numbering_map[numbering_style]
        st.session_state.current_numbering_style = numbering_code

        st.divider()

        # ==========================================
        # COMPRESSION SETTINGS
        # ==========================================
        st.header("🗜️ PDF Compression")

        if not COMPRESSION_AVAILABLE:
            st.warning("⚠️ Compression not available. Install PyMuPDF: `pip install PyMuPDF`")
            enable_compression = False
        else:
            enable_compression = st.checkbox(
                "Enable PDF Compression",
                value=True,
                help="Compress PDFs to reduce file size (50-75% reduction)"
            )

            if enable_compression:
                st.markdown('<div class="info-box">✓ Compression enabled - files will be 50-75% smaller</div>', unsafe_allow_html=True)

                # Quality preset
                quality_preset = st.selectbox(
                    "Compression Quality",
                    ["High Quality (USCIS Recommended)", "Balanced", "Maximum Compression"],
                    help="High = 300 DPI text, 200 DPI images (best for legal docs)\n"
                         "Balanced = 150 DPI images (good compression)\n"
                         "Maximum = 100 DPI images (smallest files)"
                )

                # Convert to code
                quality_map = {
                    "High Quality (USCIS Recommended)": "high",
                    "Balanced": "balanced",
                    "Maximum Compression": "maximum"
                }
                quality_code = quality_map[quality_preset]

                # Show quality details
                quality_info = {
                    "high": "📊 Text: 300 DPI | Images: 200 DPI | JPEG: 85%",
                    "balanced": "📊 Text: 300 DPI | Images: 150 DPI | JPEG: 80%",
                    "maximum": "📊 Text: 200 DPI | Images: 100 DPI | JPEG: 75%"
                }
                st.caption(quality_info[quality_code])

                # Compression method display
                with st.expander("ℹ️ Compression Methods"):
                    st.write("**3-Tier Fallback System:**")
                    st.write("1️⃣ **Ghostscript** (FREE) - 60-90% compression")
                    st.write("   Best quality/size ratio")
                    st.write("")
                    st.write("2️⃣ **PyMuPDF** (FREE) - 30-60% compression")
                    st.write("   Automatic fallback")
                    st.write("")
                    st.write("3️⃣ **SmallPDF API** (PAID) - 40-80% compression")
                    st.write("   Premium quality ($12/month)")

                # Optional SmallPDF API key
                with st.expander("🔑 SmallPDF API Key (Optional)"):
                    st.caption("For premium Tier 3 compression backup")
                    smallpdf_key = st.text_input(
                        "SmallPDF API Key",
                        type="password",
                        help="Get key at https://smallpdf.com/developers\n"
                             "Leave empty to use free compression only"
                    )
                    if smallpdf_key:
                        st.success("✓ SmallPDF API key set")
                    else:
                        st.info("Using free compression (Ghostscript + PyMuPDF)")
            else:
                quality_code = "high"
                smallpdf_key = None

        st.divider()

        # Additional options
        st.header("📋 Options")

        add_toc = st.checkbox(
            "Generate Table of Contents",
            value=True,
            help="Create a TOC page listing all exhibits"
        )

        add_archive = st.checkbox(
            "Archive URLs (archive.org)",
            value=False,
            help="Preserve URLs on archive.org (for media articles)"
        )

        merge_pdfs = st.checkbox(
            "Merge into single PDF",
            value=True,
            help="Combine all exhibits into one file"
        )

        st.divider()

        # Documentation reference
        st.header("📚 Documentation")

        with st.expander("ℹ️ Exhibit Organization Guide"):
            st.markdown("""
            **Reference Document:**
            `VISA_EXHIBIT_RAG_COMPREHENSIVE_INSTRUCTIONS.md`

            This application follows USCIS-compliant exhibit structures:

            **Supported Visa Types:**
            - ✅ O-1A (Extraordinary Ability)
            - ✅ O-1B (Arts/Entertainment)
            - ✅ P-1A (Internationally Recognized Athletes)
            - ✅ EB-1A (Permanent Residence)

            **Key Features:**
            - Standard vs Comparable Evidence (O-1A, O-1B, EB-1A only)
            - Criterion-based organization
            - USCIS regulatory compliance
            - Production-tested structures

            ⚠️ **Important:** P-1A has NO comparable evidence provision
            """)

    tab1, tab2, tab3 = st.columns(3)

    with tab1:
        if st.button("📁 Upload Files", use_container_width=True, key="tab1_active"):
            st.session_state.selected_tab = "📁 Upload Files"
            st.rerun()

    with tab2:
        if st.button("☁️ Google Drive", use_container_width=True, key="tab2_active"):
            st.session_state.selected_tab = "☁️ Google Drive"
            st.rerun()

    with tab3:
        if st.button("📊 Results", use_container_width=True, key="tab3_active"):
            st.session_state.selected_tab = "📊 Results"
            st.rerun()
            
    current_tab = st.radio(
        "",
        ["📁 Upload Files", "☁️ Google Drive", "📊 Results"],
        index=["📁 Upload Files", "☁️ Google Drive", "📊 Results"].index(
            st.session_state.selected_tab
        ),
        label_visibility="hidden"
    )
    st.markdown("""
    <style>
        div.stRadio { 
            display: none !important;
        }
        div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
            display: flex !important;
            justify-content: center !important;
        }

        .stButton > button.st-emotion-cache-5qfegl {
            background: transparent !important;
            border: none !important;
            border-radius: 0 !important;
            box-shadow: none !important;

            padding: 6px 12px !important;

            font-size: 15px !important;
            font-weight: 500 !important;
            color: #595959 !important;
            cursor: pointer !important;

            display: flex !important;
            align-items: center !important;     
            justify-content: center !important;

            border-bottom: 2px solid transparent !important;
        }

        .stButton > button.st-emotion-cache-5qfegl:hover {
            color: #1890ff !important;
            border-bottom: 2px solid #d9d9d9 !important;
        }

        .element-container.st-key-tab1_active button,
        .element-container.st-key-tab2_active button,
        .element-container.st-key-tab3_active button {
            color: #1890ff !important;
            font-weight: 600 !important;
            border-bottom: 3px solid #1890ff !important;
        }

        .stButton > button:focus {
            box-shadow: none !important;
            outline: none !important;
        }
    </style>
        """, unsafe_allow_html=True)





    # ==========================================
    # TAB 1: FILE UPLOAD
    # ==========================================
    if current_tab == "📁 Upload Files":
        st.header("Upload PDF Files")

        upload_method = st.radio(
            "Upload Method",
            ["Individual PDFs", "ZIP Archive", "URL Links", "Folder"],
            horizontal=True
        )

        uploaded_files = []
        if upload_method == "Individual PDFs":
            uploaded_files = st.file_uploader(
                "Select PDF files",
                type=["pdf"],
                accept_multiple_files=True,
                help="Upload one or more PDF files"
            )

        elif upload_method == "ZIP Archive":
            zip_file = st.file_uploader(
                "Select ZIP file",
                type=["zip"],
                help="Upload a ZIP file containing PDFs"
            )

            if zip_file:
                # Store ZIP file bytes in session state for later processing
                zip_bytes = zip_file.read()
                st.session_state.zip_file_data = zip_bytes
                st.session_state.zip_file_name = zip_file.name
                
                # Quick preview: count PDFs in ZIP without extracting
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp_zip:
                        tmp_zip.write(zip_bytes)
                        tmp_zip_path = tmp_zip.name
                    
                    pdf_count = 0
                    with zipfile.ZipFile(tmp_zip_path, 'r') as zip_ref:
                        for name in zip_ref.namelist():
                            if name.lower().endswith('.pdf'):
                                pdf_count += 1
                    
                    os.unlink(tmp_zip_path)  # Clean up temp file
                    st.info(f"Found {pdf_count} PDF file(s) in ZIP")
                    st.session_state.zip_pdf_count = pdf_count
                except Exception as e:
                    st.warning(f"Could not preview ZIP contents: {str(e)}")
                    st.session_state.zip_pdf_count = 0

        elif upload_method == "URL Links":
            st.info("💡 Paste PDF URLs (one per line) or a Google Drive folder link to extract PDFs")
            
            # Option to use Google Drive folder
            use_drive_folder = st.checkbox(
                "Extract PDFs from Google Drive folder",
                help="Check this if you're pasting a Google Drive folder link"
            )
            
            if use_drive_folder:
                folder_url = st.text_input(
                    "Google Drive Folder URL",
                    placeholder="https://drive.google.com/drive/folders/1Fj3Ueug2h6o_yuP9rHHq-upeuKDf0QBt",
                    help="Paste your Google Drive folder URL. The app will extract all PDF file URLs from the folder."
                )
                
                if folder_url:
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button("🔍 Extract PDF URLs from Folder", type="primary", use_container_width=True):
                            with st.spinner("🔄 Extracting PDF URLs from Google Drive folder..."):
                                try:
                                    # Try with OAuth if authenticated, otherwise public
                                    use_oauth = st.session_state.drive_authenticated
                                    pdf_urls = extract_pdf_urls_from_drive_folder(
                                        folder_url,
                                        use_oauth=use_oauth,
                                        client_id=st.session_state.drive_client_id if use_oauth else None,
                                        client_secret=st.session_state.drive_client_secret if use_oauth else None,
                                        credentials_token=st.session_state.drive_credentials if use_oauth else None
                                    )
                                    
                                    if pdf_urls:
                                        st.session_state.url_list = pdf_urls
                                        st.success(f"✅ Extracted {len(pdf_urls)} PDF URL(s) from folder")
                                        st.rerun()
                                    else:
                                        st.warning("⚠️ No PDF files found in the folder")
                                        st.session_state.url_list = []
                                except Exception as e:
                                    error_msg = str(e)
                                    st.error(f"❌ Error extracting URLs: {error_msg}")
                                    if "authentication" in error_msg.lower() or "private" in error_msg.lower():
                                        st.info("💡 **Tip:** The folder may be private. Expand 'Google Drive Authentication' in the Google Drive tab to authenticate.")
                                    st.session_state.url_list = []
                    
                    with col2:
                        if st.button("🔄 Clear", use_container_width=True):
                            st.session_state.url_list = []
                            st.rerun()
                
                # Show extracted URLs
                if st.session_state.url_list:
                    st.divider()
                    st.subheader("📄 Extracted PDF URLs")
                    st.success(f"✓ {len(st.session_state.url_list)} PDF URL(s) ready")
                    with st.expander("View extracted URLs"):
                        for i, url in enumerate(st.session_state.url_list, 1):
                            st.write(f"{i}. {url}")
            else:
                # Regular URL input
                url_input = st.text_area(
                    "PDF URLs",
                    placeholder="https://example.com/document1.pdf\nhttps://example.com/document2.pdf\nhttps://drive.google.com/file/d/...",
                    help="Enter one PDF URL per line. Supports direct PDF links and Google Drive file links.",
                    height=150
                )
                
                if url_input:
                    urls = [url.strip() for url in url_input.split('\n') if url.strip()]
                    if urls:
                        st.info(f"📋 {len(urls)} URL(s) entered")
                        with st.expander("View URLs"):
                            for i, url in enumerate(urls, 1):
                                st.write(f"{i}. {url}")
                        
                        # Store URLs in session state
                        st.session_state.url_list = urls
                    else:
                        st.session_state.url_list = []
        
        elif upload_method == "Folder":
            st.info("💡 Tip: Use Google Drive tab for folder processing")

        # Show uploaded files
        if uploaded_files:
            st.success(f"✓ {len(uploaded_files)} files uploaded")

            # with st.expander("📄 View uploaded files"):
            #     for i, file in enumerate(uploaded_files, 1):
            #         st.write(f"{i}. {file.name} ({file.size / 1024:.1f} KB)")

        # Generate button
        zip_ready = (upload_method == "ZIP Archive" and 
                    'zip_file_data' in st.session_state and 
                    st.session_state.zip_pdf_count > 0)
        url_ready = (upload_method == "URL Links" and 
                    'url_list' in st.session_state and 
                    len(st.session_state.url_list) > 0)
        
        if uploaded_files or zip_ready or url_ready:
            st.divider()
            
            if st.button("🚀 Generate Exhibits", type="primary", use_container_width=True):
                # Handle URL downloads
                if upload_method == "URL Links" and url_ready:
                    # Download URLs and process them
                    generate_exhibits_from_urls(
                        st.session_state.url_list,
                        visa_type,
                        numbering_code,
                        enable_compression,
                        quality_code,
                        smallpdf_key if enable_compression else None,
                        add_toc,
                        add_archive,
                        merge_pdfs
                    )
                else:
                    # Handle ZIP file extraction during processing
                    files_to_process = uploaded_files if uploaded_files else None
                    is_zip = (upload_method == "ZIP Archive" and 'zip_file_data' in st.session_state)
                    
                    generate_exhibits(
                        files_to_process,
                        visa_type,
                        numbering_code,
                        enable_compression,
                        quality_code,
                        smallpdf_key if enable_compression else None,
                        add_toc,
                        add_archive,
                        merge_pdfs,
                        is_zip=is_zip
                    )

    # ==========================================
    # TAB 2: GOOGLE DRIVE
    # ==========================================
    elif current_tab == "☁️ Google Drive":
        st.header("☁️ Google Drive Integration")
        st.info("💡 Connect to Google Drive to process folders directly. Public folders work without authentication!")

        # OAuth2 Authentication Section (Optional - only for private folders)
        with st.expander("🔐 Google Drive Authentication (Optional - for private folders only)", expanded=False):
            st.markdown("""
            **Setup Instructions:**
            1. Go to [Google Cloud Console](https://console.cloud.google.com/)
            2. Create a project or select existing
            3. Enable **Google Drive API** (APIs & Services → Library → Google Drive API → Enable)
            4. Go to **APIs & Services → Credentials**
            5. Click **Create Credentials → OAuth 2.0 Client ID**
            6. Application type: **Web application**
            7. Add authorized redirect URI: `urn:ietf:wg:oauth:2.0:oob`
            8. Copy **Client ID** and **Client Secret** below
            """)

            client_id = st.text_input(
                "Client ID",
                value=st.session_state.drive_client_id,
                help="Your Google OAuth2 Client ID"
            )
            st.session_state.drive_client_id = client_id

            client_secret = st.text_input(
                "Client Secret",
                value=st.session_state.drive_client_secret,
                type="password",
                help="Your Google OAuth2 Client Secret"
            )
            st.session_state.drive_client_secret = client_secret

            # Authentication status
            if st.session_state.drive_authenticated:
                st.success("✅ Authenticated with Google Drive")
                if st.button("🔓 Disconnect", type="secondary"):
                    st.session_state.drive_authenticated = False
                    st.session_state.drive_credentials = None
                    st.session_state.drive_files_loaded = []
                    st.session_state.oauth_auth_url = None
                    st.rerun()
            else:
                if client_id and client_secret:
                    # Start OAuth flow
                    if st.button("🔗 Authorize Google Drive", type="primary"):
                        try:
                            drive_handler = GoogleDriveHandler(
                                client_id=client_id,
                                client_secret=client_secret
                            )
                            auth_url, _ = drive_handler.get_authorization_url()
                            st.session_state.oauth_auth_url = auth_url
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error starting authorization: {str(e)}")
                    
                    # Show authorization URL if we have it
                    if st.session_state.oauth_auth_url:
                        st.markdown("### 📋 Authorization Steps:")
                        
                        with st.expander("ℹ️ What is OAuth and why do I need this?", expanded=False):
                            st.markdown("""
                            **OAuth2 is Google's secure way to let apps access your Drive files.**
                            
                            **Why it's needed:**
                            - 🔒 **Security**: Google never gives apps your password
                            - ✅ **Control**: You choose what the app can access (read-only in this case)
                            - 🔑 **Temporary**: You can revoke access anytime
                            
                            **What happens:**
                            1. You click the authorization link
                            2. Google shows you what permissions the app wants
                            3. You approve (or deny)
                            4. Google gives you a temporary code
                            5. The app exchanges the code for access tokens
                            6. The app can now read your Drive files (but never your password)
                            
                            **Is this safe?**
                            - ✅ Yes! This is Google's official, secure method
                            - ✅ The app only gets "read-only" access (can't delete or modify)
                            - ✅ You can revoke access anytime in your Google Account settings
                            - ✅ No passwords are shared
                            """)
                        
                        st.markdown("""
                        **Follow these steps:**
                        
                        1. **Click the link below** - Opens Google's authorization page
                        2. **Sign in** - Use your Google account (the one that owns the Drive folder)
                        3. **Review permissions** - You'll see "View and download files in Google Drive"
                        4. **Click "Allow"** - This grants read-only access
                        5. **Copy the code** - Google will show a code like `4/0Aean...` 
                        6. **Paste it below** - Return here and paste the code
                        7. **Click "Complete"** - The app will finish the setup
                        """)
                        
                        st.markdown(f"[🔗 **Click here to authorize**]({st.session_state.oauth_auth_url})")
                        
                        st.caption("💡 **Tip**: The authorization page will open in a new tab. After you approve, copy the code and return here.")
                        
                        st.divider()
                        st.markdown("**Step 5-7: Enter the authorization code**")
                        st.caption("After clicking 'Allow' on Google's page, you'll see a code. Copy it and paste below:")
                        
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            auth_code = st.text_input(
                                "Authorization Code",
                                placeholder="Paste the code here (e.g., 4/0Aean...)",
                                help="The code will look like: 4/0AeanAbCdEfGhIjKlMnOpQrStUvWxYz123456789",
                                label_visibility="collapsed"
                            )
                        with col2:
                            if st.button("✅ Complete Authorization", type="primary"):
                                if auth_code:
                                    with st.spinner("Completing authorization..."):
                                        try:
                                            drive_handler = GoogleDriveHandler(
                                                client_id=client_id,
                                                client_secret=client_secret
                                            )
                                            token_dict = drive_handler.complete_oauth2_flow(auth_code)
                                            st.session_state.drive_credentials = token_dict
                                            st.session_state.drive_authenticated = True
                                            st.session_state.oauth_auth_url = None
                                            st.success("✅ Successfully authenticated!")
                                            st.info("🎉 You're all set! You can now load files from private Google Drive folders.")
                                            st.rerun()
                                        except Exception as e:
                                            error_msg = str(e)
                                            st.error(f"❌ Authentication failed: {error_msg}")
                                            if "invalid_grant" in error_msg.lower():
                                                st.warning("💡 The code may have expired. Please click 'Authorize Google Drive' again to get a new code.")
                                            elif "invalid_client" in error_msg.lower():
                                                st.warning("💡 Please check that your Client ID and Client Secret are correct.")
                                else:
                                    st.warning("⚠️ Please enter the authorization code first")
                        
                        st.info("""
                        **What happens after you complete authorization:**
                        - ✅ The app stores a secure access token (not your password!)
                        - ✅ You can now access private Google Drive folders
                        - ✅ The token is saved in your session (cleared when you close the app)
                        - ✅ You can revoke access anytime in [Google Account Settings](https://myaccount.google.com/permissions)
                        """)
                else:
                    st.info("👆 Enter your Client ID and Client Secret to begin")

        # Google Drive Folder Processing
        st.divider()
        
        drive_url = st.text_input(
            "Google Drive Folder URL",
            value="https://drive.google.com/drive/u/0/folders/1Fj3Ueug2h6o_yuP9rHHq-upeuKDf0QBt",
            placeholder="https://drive.google.com/drive/folders/...",
            help="Paste the URL of your Google Drive folder containing PDFs. For public folders, no authentication needed!"
        )

        if drive_url:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("📥 Load from Drive", type="primary", use_container_width=True):
                    with st.spinner("🔄 Loading files from Google Drive..."):
                        try:
                            # Try public access first (no OAuth needed)
                            drive_handler = GoogleDriveHandler()
                            
                            try:
                                # Try public folder access
                                files = drive_handler.list_folder_files_public(
                                    drive_url,
                                    file_types=['application/pdf']
                                )
                                st.info("📂 Accessing folder as public (no authentication required)")
                            except Exception as public_error:
                                # Public access failed, try OAuth if authenticated
                                if st.session_state.drive_authenticated:
                                    # Use OAuth
                                    drive_handler = GoogleDriveHandler(
                                        client_id=st.session_state.drive_client_id,
                                        client_secret=st.session_state.drive_client_secret,
                                        credentials_token=st.session_state.drive_credentials
                                    )
                                    files = drive_handler.list_folder_files(
                                        drive_url,
                                        file_types=['application/pdf']
                                    )
                                    st.info("🔐 Accessing folder with OAuth authentication")
                                else:
                                    # No OAuth, show helpful error
                                    raise Exception(
                                        f"Folder is private or not accessible.\n\n"
                                        f"**Options:**\n"
                                        f"1. Make the folder public: Right-click folder → Share → 'Anyone with the link can view'\n"
                                        f"2. Or authenticate with OAuth2 above (expand 'Google Drive Authentication')"
                                    )
                            
                            if not files:
                                st.warning("⚠️ No PDF files found in the specified folder")
                                st.info("""
                                **Possible reasons:**
                                - The folder might not be fully public (check sharing settings)
                                - PDF files might be in subfolders (currently only top-level files are scanned)
                                - Google Drive's HTML structure may have changed
                                
                                **Solution:** Use OAuth2 authentication (expand 'Google Drive Authentication' above) for more reliable access.
                                """)
                                st.session_state.drive_files_loaded = []
                            else:
                                st.session_state.drive_files_loaded = files
                                st.success(f"✅ Found {len(files)} PDF file(s) in folder")
                                st.rerun()
                                
                        except Exception as e:
                            error_msg = str(e)
                            st.error(f"❌ {error_msg}")
                            
                            # Provide helpful suggestions
                            if "private" in error_msg.lower() or "authentication" in error_msg.lower():
                                st.info("💡 **Tip:** For private folders, expand 'Google Drive Authentication' above and complete OAuth setup")
                            elif "not publicly accessible" in error_msg.lower() or len(st.session_state.drive_files_loaded) == 0:
                                st.warning("""
                                **Public folder access may be unreliable due to Google Drive's dynamic loading.**
                                
                                **Recommended solutions:**
                                1. **Use OAuth2** (most reliable): Expand 'Google Drive Authentication' above
                                2. **Verify folder is public**: Right-click folder → Share → Ensure "Anyone with the link can view"
                                3. **Check folder contents**: Make sure PDFs are in the root of the folder (not in subfolders)
                                """)
                            
                            with st.expander("Error Details"):
                                st.code(error_msg)
                            st.session_state.drive_files_loaded = []
            
            with col2:
                if st.button("🔄 Clear", use_container_width=True):
                    st.session_state.drive_files_loaded = []
                    st.rerun()

                # Show loaded files
                if st.session_state.drive_files_loaded:
                    st.divider()
                    st.subheader("📄 Files Loaded from Drive")
                    st.success(f"✓ {len(st.session_state.drive_files_loaded)} PDF file(s) loaded")
                    
                    with st.expander("View loaded files"):
                        for i, file_info in enumerate(st.session_state.drive_files_loaded, 1):
                            file_size = file_info.get('size', 0)
                            size_str = f" ({file_size / (1024*1024):.2f} MB)" if file_size > 0 else ""
                            st.write(f"{i}. {file_info['name']}{size_str}")

                    st.divider()
                    
                    if st.button("🚀 Generate Exhibits from Drive", type="primary", use_container_width=True):
                        # Pass OAuth credentials only if authenticated
                        client_id = st.session_state.drive_client_id if st.session_state.drive_authenticated else None
                        client_secret = st.session_state.drive_client_secret if st.session_state.drive_authenticated else None
                        credentials_token = st.session_state.drive_credentials if st.session_state.drive_authenticated else None
                        
                        generate_exhibits_from_drive(
                            st.session_state.drive_files_loaded,
                            visa_type,
                            numbering_code,
                            enable_compression,
                            quality_code,
                            smallpdf_key if enable_compression else None,
                            add_toc,
                            add_archive,
                            merge_pdfs,
                            client_id,
                            client_secret,
                            credentials_token
                        )

    # ==========================================
    # TAB 3: RESULTS
    # ==========================================
    elif current_tab == "📊 Results":
        st.header("Generation Results")

        if st.session_state.exhibits_generated:
            # Success message
            st.markdown('<div class="success-box">✓ Exhibits generated successfully!</div>', unsafe_allow_html=True)

            # Statistics
            st.subheader("📊 Statistics")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.markdown(f"""
                <div class="stat-card">
                    <div class="stat-value">{len(st.session_state.exhibit_list)}</div>
                    <div class="stat-label">Exhibits</div>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                total_pages = sum(ex.get('pages', 0) for ex in st.session_state.exhibit_list)
                st.markdown(f"""
                <div class="stat-card">
                    <div class="stat-value">{total_pages}</div>
                    <div class="stat-label">Total Pages</div>
                </div>
                """, unsafe_allow_html=True)

            with col3:
                if st.session_state.compression_stats:
                    reduction = st.session_state.compression_stats.get('avg_reduction', 0)
                    st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-value">{reduction:.1f}%</div>
                        <div class="stat-label">Size Reduction</div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-value">-</div>
                        <div class="stat-label">Compression</div>
                    </div>
                    """, unsafe_allow_html=True)

            with col4:
                if st.session_state.compression_stats:
                    original_mb = st.session_state.compression_stats.get('original_size', 0) / (1024*1024)
                    compressed_mb = st.session_state.compression_stats.get('compressed_size', 0) / (1024*1024)
                    st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-value">{compressed_mb:.1f} MB</div>
                        <div class="stat-label">Final Size</div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-value">-</div>
                        <div class="stat-label">File Size</div>
                    </div>
                    """, unsafe_allow_html=True)

            # Compression details
            if st.session_state.compression_stats:
                st.divider()
                st.subheader("🗜️ Compression Details")

                stats = st.session_state.compression_stats

                col1, col2 = st.columns(2)

                with col1:
                    st.metric(
                        "Original Size",
                        f"{stats.get('original_size', 0) / (1024*1024):.2f} MB"
                    )
                    st.metric(
                        "Compressed Size",
                        f"{stats.get('compressed_size', 0) / (1024*1024):.2f} MB",
                        delta=f"-{stats.get('avg_reduction', 0):.1f}%",
                        delta_color="inverse"
                    )

                with col2:
                    st.metric(
                        "Compression Method",
                        stats.get('method', 'Unknown').title()
                    )
                    st.metric(
                        "Quality Preset",
                        stats.get('quality', 'Unknown').title()
                    )

            # Exhibit list with pagination and reordering
            st.divider()
            st.subheader("📋 Exhibit List")
            
            # Reorder controls
            if len(st.session_state.exhibit_list) > 1:
                st.info("💡 **Reorder exhibits:** Use ↑/↓ buttons to move exhibits up or down. Numbers will update automatically.")
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("🔄 Reset to Original Order", help="Reset to original order", use_container_width=True):
                        # Re-sort by original index if available, otherwise keep current
                        if any('_original_index' in ex for ex in st.session_state.exhibit_list):
                            st.session_state.exhibit_list.sort(key=lambda x: x.get('_original_index', 0))
                        st.rerun()
                with col2:
                    st.caption("Note: Reordering updates display. Regenerate PDF to apply new order to final package.")
            
            # Pagination settings
            items_per_page = 10
            total_exhibits = len(st.session_state.exhibit_list)
            total_pages = (total_exhibits + items_per_page - 1) // items_per_page if total_exhibits > 0 else 1
            
            # Ensure page is valid
            if st.session_state.exhibit_page > total_pages:
                st.session_state.exhibit_page = 1
            if st.session_state.exhibit_page < 1:
                st.session_state.exhibit_page = 1
            
            # Pagination controls
            if total_pages > 1:
                col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
                with col1:
                    if st.button("◀️ Prev", disabled=(st.session_state.exhibit_page == 1)):
                        st.session_state.exhibit_page -= 1
                        st.rerun()
                with col2:
                    if st.button("Next ▶️", disabled=(st.session_state.exhibit_page == total_pages)):
                        st.session_state.exhibit_page += 1
                        st.rerun()
                with col3:
                    st.caption(f"Page {st.session_state.exhibit_page} of {total_pages} ({total_exhibits} total exhibits)")
                with col4:
                    page_input = st.number_input(
                        "Go to page",
                        min_value=1,
                        max_value=total_pages,
                        value=st.session_state.exhibit_page,
                        key="page_jump",
                        label_visibility="collapsed"
                    )
                    if page_input != st.session_state.exhibit_page:
                        st.session_state.exhibit_page = page_input
                        st.rerun()
            
            # Calculate pagination range
            start_idx = (st.session_state.exhibit_page - 1) * items_per_page
            end_idx = min(start_idx + items_per_page, total_exhibits)
            displayed_exhibits = st.session_state.exhibit_list[start_idx:end_idx]
            
            # Display exhibits in table format with action buttons integrated
            if total_exhibits > 0:
                # Create table header with action column
                header_col1, header_col2, header_col3, header_col4, header_col5, header_col6 = st.columns([0.35, 1.8, 1.8, 0.6, 0.8, 1.2])
                
                with header_col1:
                    st.markdown("**#**")
                with header_col2:
                    st.markdown("**Title**")
                with header_col3:
                    st.markdown("**File**")
                with header_col4:
                    st.markdown("**Pages**")
                with header_col5:
                    st.markdown("**Compression**")
                with header_col6:
                    st.markdown("**Actions**")
                
                st.divider()
                
                # Display each exhibit row with action buttons
                for idx, exhibit in enumerate(displayed_exhibits):
                    actual_idx = start_idx + idx
                    move_up_disabled = (actual_idx == 0)
                    move_down_disabled = (actual_idx == total_exhibits - 1)
                    
                    row_col1, row_col2, row_col3, row_col4, row_col5, row_col6 = st.columns([0.35, 1.8, 1.8, 0.6, 0.8, 1.2])
                    
                    compression_info = "-"
                    if 'compression' in exhibit and exhibit['compression']:
                        compression_info = f"{exhibit['compression']['reduction']:.1f}%"
                    
                    with row_col1:
                        st.write(exhibit['number'])
                    
                    with row_col2:
                        title_display = exhibit['title'][:35] + "..." if len(exhibit['title']) > 35 else exhibit['title']
                        st.write(title_display)
                    
                    with row_col3:
                        file_display = exhibit['filename'][:30] + "..." if len(exhibit['filename']) > 30 else exhibit['filename']
                        st.write(file_display)
                    
                    with row_col4:
                        st.write(exhibit.get('pages', '-'))
                    
                    with row_col5:
                        st.write(compression_info)
                    
                    with row_col6:
                        btn_col1, btn_col2 = st.columns([1, 1], gap="small")
                        with btn_col1:
                            if st.button("↑", key=f"up_{actual_idx}", disabled=move_up_disabled, help="Move up"):
                                st.session_state.exhibit_list[actual_idx], st.session_state.exhibit_list[actual_idx - 1] = \
                                    st.session_state.exhibit_list[actual_idx - 1], st.session_state.exhibit_list[actual_idx]
                                numbering_style = st.session_state.current_numbering_style
                                for i, ex in enumerate(st.session_state.exhibit_list):
                                    if numbering_style == "letters":
                                        ex['number'] = chr(65 + i)
                                    elif numbering_style == "numbers":
                                        ex['number'] = str(i + 1)
                                    else:
                                        ex['number'] = to_roman(i + 1)
                                st.rerun()
                        
                        with btn_col2:
                            if st.button("↓", key=f"down_{actual_idx}", disabled=move_down_disabled, help="Move down"):
                                st.session_state.exhibit_list[actual_idx], st.session_state.exhibit_list[actual_idx + 1] = \
                                    st.session_state.exhibit_list[actual_idx + 1], st.session_state.exhibit_list[actual_idx]
                                numbering_style = st.session_state.current_numbering_style
                                for i, ex in enumerate(st.session_state.exhibit_list):
                                    if numbering_style == "letters":
                                        ex['number'] = chr(65 + i)
                                    elif numbering_style == "numbers":
                                        ex['number'] = str(i + 1)
                                    else:
                                        ex['number'] = to_roman(i + 1)
                                st.rerun()
            else:
                st.info("No exhibits to display")

            # Download button
            st.divider()
            if 'output_file' in st.session_state:
                output_file_path = st.session_state.output_file
                if os.path.exists(output_file_path):
                    try:
                        with open(output_file_path, 'rb') as f:
                            file_data = f.read()
                            if file_data:
                                st.download_button(
                                    label="📥 Download Exhibit Package",
                                    data=file_data,
                                    file_name=f"Exhibit_Package_{visa_type}_{datetime.now().strftime('%Y%m%d')}.pdf",
                                    mime="application/pdf",
                                    type="primary",
                                    use_container_width=True
                                )
                            else:
                                st.error("❌ Output file is empty")
                    except Exception as e:
                        st.error(f"❌ Error reading output file: {str(e)}")
                        with st.expander("Debug Info"):
                            st.write(f"File path: {output_file_path}")
                            st.write(f"File exists: {os.path.exists(output_file_path)}")
                else:
                    st.error(f"❌ Output file not found at: {output_file_path}")
                    st.info("Please regenerate the exhibits to create a new PDF.")
            else:
                st.warning("⚠️ No output file available. Please ensure 'Merge into single PDF' is enabled and regenerate exhibits.")
        else:
            st.info("👈 Upload files and click 'Generate Exhibits' to see results here")

def generate_exhibits(
    files,
    visa_type: str,
    numbering_style: str,
    enable_compression: bool,
    quality_preset: str,
    smallpdf_api_key: Optional[str],
    add_toc: bool,
    add_archive: bool,
    merge_pdfs: bool,
    is_zip: bool = False
):
    """Generate exhibit package from uploaded files"""

    with st.spinner("🔄 Processing files..."):
        try:
            # Create PDF handler with compression settings
            pdf_handler = PDFHandler(
                enable_compression=enable_compression,
                quality_preset=quality_preset,
                smallpdf_api_key=smallpdf_api_key
            )

            # Create temporary directory for processing
            with tempfile.TemporaryDirectory() as tmp_dir:
                # Save uploaded files
                file_paths = []

                progress_bar = st.progress(0)
                status_text = st.empty()

                # Handle ZIP file extraction
                if is_zip and 'zip_file_data' in st.session_state:
                    status_text.text("📦 Extracting ZIP file...")
                    zip_bytes = st.session_state.zip_file_data
                    zip_path = os.path.join(tmp_dir, "upload.zip")
                    
                    # Write ZIP to temp location
                    with open(zip_path, 'wb') as f:
                        f.write(zip_bytes)
                    
                    # Extract ZIP
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(tmp_dir)
                    
                    # Find all PDF files in extracted ZIP
                    pdf_files = list(Path(tmp_dir).rglob("*.pdf"))
                    
                    if pdf_files:
                        file_paths = [str(p) for p in pdf_files]
                        status_text.text(f"✓ Extracted {len(file_paths)} PDF file(s) from ZIP")
                        progress_bar.progress(0.1)  # Show progress after extraction
                    else:
                        st.error("❌ No PDF files found in ZIP archive")
                        return
                
                # Handle regular file upload
                elif files:
                    # Handle file upload
                    for i, file in enumerate(files):
                        if hasattr(file, 'name'):  # Streamlit uploaded file
                            file_path = os.path.join(tmp_dir, file.name)
                            with open(file_path, 'wb') as f:
                                f.write(file.read())
                        else:  # File path (shouldn't happen now, but keeping for safety)
                            file_path = file

                        file_paths.append(file_path)
                        progress_bar.progress((i + 1) / len(files))
                        status_text.text(f"Saving file {i+1}/{len(files)}")

                    status_text.text("✓ All files saved")
                else:
                    st.error("❌ No files to process")
                    return

                # Compression phase
                compression_results = []
                total_original_size = 0
                total_compressed_size = 0

                if enable_compression:
                    status_text.text("🗜️ Compressing PDFs...")
                    
                    # Validate all files exist before compression
                    missing_files = [f for f in file_paths if not os.path.exists(f)]
                    if missing_files:
                        st.error(f"❌ Cannot compress: {len(missing_files)} file(s) not found")
                        for missing in missing_files:
                            st.write(f"  - {missing}")
                        return

                    for i, file_path in enumerate(file_paths):
                        if not os.path.exists(file_path):
                            st.warning(f"⚠️ File not found, skipping: {file_path}")
                            continue
                            
                        result = pdf_handler.compressor.compress(file_path) if pdf_handler.compressor else {'success': False}

                        if result['success']:
                            compression_results.append(result)
                            total_original_size += result['original_size']
                            total_compressed_size += result['compressed_size']

                            status_text.text(
                                f"🗜️ Compressed {i+1}/{len(file_paths)}: "
                                f"{result['reduction_percent']:.1f}% reduction ({result['method']})"
                            )

                        progress_bar.progress((i + 1) / len(file_paths))

                    # Calculate average compression
                    if compression_results:
                        avg_reduction = (1 - total_compressed_size / total_original_size) * 100 if total_original_size > 0 else 0

                        st.session_state.compression_stats = {
                            'original_size': total_original_size,
                            'compressed_size': total_compressed_size,
                            'avg_reduction': avg_reduction,
                            'method': compression_results[0]['method'] if compression_results else 'none',
                            'quality': quality_preset
                        }

                        status_text.text(f"✓ Compression complete: {avg_reduction:.1f}% average reduction")

                # Number exhibits
                status_text.text("📝 Numbering exhibits...")

                exhibit_list = []
                numbered_files = []

                for i, file_path in enumerate(file_paths):
                    # Get exhibit number
                    if numbering_style == "letters":
                        exhibit_num = chr(65 + i)  # A, B, C...
                    elif numbering_style == "numbers":
                        exhibit_num = str(i + 1)  # 1, 2, 3...
                    else:  # roman
                        exhibit_num = to_roman(i + 1)  # I, II, III...

                    # Add exhibit number to PDF
                    numbered_file = pdf_handler.add_exhibit_number(file_path, exhibit_num)
                    numbered_files.append(numbered_file)

                    # Track exhibit info
                    exhibit_info = {
                        'number': exhibit_num,
                        'title': Path(file_path).stem,
                        'filename': os.path.basename(file_path),
                        'pages': get_pdf_page_count(file_path)
                    }

                    # Add compression info if available
                    if i < len(compression_results) and compression_results[i]['success']:
                        exhibit_info['compression'] = {
                            'reduction': compression_results[i]['reduction_percent'],
                            'method': compression_results[i]['method']
                        }

                    exhibit_list.append(exhibit_info)

                    progress_bar.progress((i + 1) / len(file_paths))
                    status_text.text(f"📝 Numbered exhibit {exhibit_num}")

                st.session_state.exhibit_list = exhibit_list

                # Generate TOC if requested
                if add_toc:
                    status_text.text("📋 Generating Table of Contents...")
                    toc_file = pdf_handler.generate_table_of_contents(
                        exhibit_list,
                        visa_type,
                        os.path.join(tmp_dir, "TOC.pdf")
                    )
                    numbered_files.insert(0, toc_file)

                # Merge PDFs if requested
                if merge_pdfs:
                    status_text.text("📦 Merging PDFs...")
                    
                    # Verify all files exist before merging
                    missing_files = [f for f in numbered_files if not os.path.exists(f)]
                    if missing_files:
                        st.error(f"❌ Cannot merge: {len(missing_files)} file(s) not found")
                        for missing in missing_files:
                            st.write(f"  - {missing}")
                    else:
                        output_file = os.path.join(tmp_dir, "final_package.pdf")
                        merged_file = pdf_handler.merge_pdfs(numbered_files, output_file)

                        # Verify the merged file was created
                        if os.path.exists(merged_file):
                            # Save to session state for download (outside temp directory)
                            final_output = os.path.join(tempfile.gettempdir(), f"exhibit_package_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
                            import shutil
                            shutil.copy(merged_file, final_output)
                            
                            # Verify copy was successful
                            if os.path.exists(final_output):
                                st.session_state.output_file = final_output
                                status_text.text(f"✓ PDF saved: {os.path.basename(final_output)}")
                            else:
                                st.error("❌ Failed to save output file")
                        else:
                            st.error(f"❌ Merged PDF not found at: {merged_file}")
                else:
                    # If not merging, still set a flag so user knows processing is complete
                    st.info("ℹ️ PDFs processed individually (merge disabled)")

                progress_bar.progress(100)
                status_text.text("✓ Generation complete!")

                st.session_state.exhibits_generated = True
                st.session_state.selected_tab = "📊 Results"
                st.session_state.show_results_message = True  # Show navigation message
                
                # Show success message with auto-navigation
                st.success("🎉 **Generation Complete!** Automatically navigating to Results tab...")
                
                st.rerun()


        except Exception as e:
            st.error(f"❌ Error generating exhibits: {str(e)}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())

def get_pdf_page_count(pdf_path: str) -> int:
    """Get number of pages in PDF"""
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(pdf_path)
        return len(reader.pages)
    except:
        return 0

def to_roman(num: int) -> str:
    """Convert number to Roman numeral"""
    val = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]
    syms = ['M', 'CM', 'D', 'CD', 'C', 'XC', 'L', 'XL', 'X', 'IX', 'V', 'IV', 'I']
    roman_num = ''
    i = 0
    while num > 0:
        for _ in range(num // val[i]):
            roman_num += syms[i]
            num -= val[i]
        i += 1
    return roman_num

if __name__ == "__main__":
    main()
