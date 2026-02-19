"""
Usage:
    python3 WARC_version_4.py "/path/to/warcs/" -o "./outputs" --workers 3   
"""

import sys
import gzip
import json
import time
import argparse
import re
import hashlib
import unicodedata
from pathlib import Path
from typing import Iterator, Optional, Dict, Tuple, Any, List, FrozenSet
from dataclasses import dataclass, field
from collections import deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from urllib.parse import urlparse
import multiprocessing

try:
    from selectolax.lexbor import LexborHTMLParser, LexborNode
except ImportError:
    print("Error: selectolax required. Install: pip install selectolax", file=sys.stderr)
    sys.exit(1)

# configuration ;)
DEFAULT_BUFFER_SIZE = 65536
DEFAULT_BATCH_SIZE = 500
DEFAULT_MIN_LENGTH = 15         
DEFAULT_MAX_LENGTH = 10_000_000
DEFAULT_MIN_WORDS = 2            
DEFAULT_MAX_LINE_REMOVAL = 0.80  

# Precompiled regex patterns
WORD_PATTERN = re.compile(r'\b\w+\b', re.UNICODE)
CHARSET_PATTERN = re.compile(b'charset=["\']?([^"\'\\\s>;)]+)', re.IGNORECASE)
HTTP_CHARSET_PATTERN = re.compile(r'charset=([^\s;]+)', re.IGNORECASE)
WHITESPACE_PATTERN = re.compile(r'[ \t]+')  # Only horizontal whitespace
MULTIPLE_NEWLINES = re.compile(r'\n{3,}')   # 3+ newlines -> 2
SENTENCE_END_PATTERN = re.compile(r'[.!?。！？:：；;][\s]*$', re.UNICODE)


COPYRIGHT_LINE_PATTERN = re.compile(r'©|copyright\s*©', re.IGNORECASE)
ADS_BY_GOOGLE_PATTERN = re.compile(r'ads\s+by\s+google', re.IGNORECASE)
POST_COMMENTS_PATTERN = re.compile(r'^\s*post\s+comments?', re.IGNORECASE)


BOILERPLATE_LINE_PATTERNS = re.compile(
    r'^('
    # Copyright & Legal
    r'copyright\s*©?|'
    r'©\s*\d{4}|'
    r'all\s+rights\s+reserved|'
    r'privacy\s+policy|'
    r'terms\s+(of\s+)?(use|service|conditions)|'
    r'cookie\s+(policy|notice|consent|preferences)|'
    r'legal\s+(notice|disclaimer)|'
    r'disclaimer|'
    r'dmca|'
    r'gdpr|'
    r'ccpa|'
    
    # Advertising & Sponsored Content
    r'powered\s+by|'
    r'sponsored\s+(by|content|post|link)|'
    r'advertisement|'
    r'advertise\s+(with\s+us|here)|'
    r'ads\s+by|'
    r'adchoices|'
    r'promoted\s+(content|stories|posts)|'
    
    # Social & Sharing
    r'follow\s+us\s*$|'
    r'share\s+(this|on|via)\s*$|'
    r'share\s+on\s+(facebook|twitter|linkedin|pinterest|whatsapp|reddit)|'
    r'tweet\s*$|'
    r'pin\s+it\s*$|'
    r'like\s+us\s+on|'
    r'connect\s+with\s+us|'
    r'join\s+our\s+(community|newsletter|mailing\s+list)|'
    
    # User Actions / Navigation
    r'subscribe\s+(to|now)\s*$|'
    r'sign\s+up\s*$|'
    r'log\s*in\s*$|'
    r'sign\s*in\s*$|'
    r'register\s*$|'
    r'create\s+an?\s+account|'
    r'forgot\s+(your\s+)?password|'
    r'reset\s+password|'
    r'skip\s+to|'
    r'jump\s+to|'
    r'back\s+to\s+top|'
    r'scroll\s+to\s+top|'
    r'click\s+here\s*$|'
    r'read\s+more\s*$|'
    r'learn\s+more\s*$|'
    r'view\s+more\s*$|'
    r'show\s+more\s*$|'
    r'see\s+(more|all)\s*$|'
    r'load\s+more\s*$|'
    r'expand\s*$|'
    r'collapse\s*$|'
    
    # Comments & Engagement Metrics
    r'\d+\s+(comment|like|share|view|reaction|reply|retweet|follower)s?\s*$|'
    r'leave\s+a\s+(comment|reply)|'
    r'post\s+(a\s+)?comment|'
    r'add\s+(a\s+)?comment|'
    r'comments?\s*(are\s+)?(closed|disabled|off)|'
    r'no\s+comments?\s*(yet)?\s*$|'
    r'be\s+the\s+first\s+to\s+comment|'
    r'what\s+do\s+you\s+think|'
    
    # Loading & State Messages
    r'loading\.{0,3}\s*$|'
    r'please\s+wait|'
    r'processing\.{0,3}\s*$|'
    r'submitting\.{0,3}\s*$|'
    r'redirecting\.{0,3}\s*$|'
    
    # Technical Requirements
    r'javascript\s+(is\s+)?(required|disabled|enabled)|'
    r'enable\s+(javascript|cookies)|'
    r'cookies\s+(are\s+)?(required|disabled)|'
    r'your\s+browser\s+(is\s+)?(not\s+supported|outdated)|'
    r'upgrade\s+your\s+browser|'
    r'best\s+viewed\s+(in|with)|'
    
    # Newsletter & Subscription
    r'enter\s+your\s+email|'
    r'subscribe\s+to\s+(our\s+)?newsletter|'
    r'get\s+(our\s+)?newsletter|'
    r'stay\s+updated|'
    r'never\s+miss\s+(a\s+)?(post|update|story)|'
    r'unsubscribe|'
    
    # Contact & Support
    r'contact\s+us|'
    r'get\s+in\s+touch|'
    r'customer\s+(support|service)|'
    r'help\s+center|'
    r'faq\s*$|'
    r'support\s+center|'
    r'post\s+a\s+comment|'
    
    # E-commerce
    r'add\s+to\s+cart|'
    r'add\s+to\s+bag|'
    r'add\s+to\s+wishlist|'
    r'buy\s+now|'
    r'shop\s+now|'
    r'order\s+now|'
    r'checkout|'
    r'free\s+shipping|'
    r'in\s+stock|'
    r'out\s+of\s+stock|'
    r'sold\s+out|'
    r'add\s+to\s+compare|'
    r'compare\s+products|'
    
    # Search Related
    r'search\s+results?\s+for|'
    r'no\s+(search\s+)?results?\s+found|'
    r'showing\s+\d+.*results|'
    r'your\s+search\s*$|'
    
    # Pagination
    r'page\s+\d+\s+of\s+\d+|'
    r'next\s+page|'
    r'previous\s+page|'
    r'first\s+page|'
    r'last\s+page|'
    r'older\s+posts?|'
    r'newer\s+posts?|'
    
    # Time/Date Boilerplate
    r'posted\s+on\s*$|'
    r'published\s+on\s*$|'
    r'updated\s+on\s*$|'
    r'last\s+modified\s*$|'
    r'\d+\s+(minute|hour|day|week|month|year)s?\s+ago\s*$|'
    
    # Author/Attribution Boilerplate
    r'written\s+by\s*$|'
    r'by\s+admin\s*$|'
    r'posted\s+by\s*$|'
    r'author\s*:\s*$|'
    
    # Related Content Navigation
    r'related\s+(posts?|articles?|stories?|content)|'
    r'you\s+may\s+(also\s+)?like|'
    r'recommended\s+for\s+you|'
    r'trending\s+(now|articles?|posts?)|'
    r'popular\s+(posts?|articles?)|'
    r'most\s+read|'
    r'editor.s\s+picks?|'
    r'featured\s+(posts?|articles?)|'
    
    # Print/Download/Save
    r'print\s+this\s+(page|article)|'
    r'download\s+as\s+pdf|'
    r'save\s+for\s+later|'
    r'bookmark\s+this|'
    
    # Social Proof
    r'\d+\s+people\s+(liked?|shared?|viewed?)\s+this|'
    r'be\s+the\s+first\s+to\s+like|'
    r'rate\s+this\s+(article|post)|'
    r'\d+\s+star\s+rating|'

    # webpage Error 
    r'Please\s+wait\s+while\s+your\s+request\s+is\s+being\s+verified|'
    
    # Miscellaneous UI/UX
    r'toggle\s+(menu|navigation|sidebar)|'
    r'close\s+(menu|popup|modal|window)|'
    r'open\s+(menu|navigation)|'
    r'menu\s*$|'
    r'table\s+of\s+contents|'
    r'quick\s+links|'
    r'site\s*map|'
    r'accessibility|'
    r'report\s+(this|abuse|spam|error)|'
    r'flag\s+as\s+(inappropriate|spam)'
    r')$',
    re.IGNORECASE | re.UNICODE
)

# Patterns for garbage/encoding issues
GARBAGE_PATTERN = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]{2,}')
REPLACEMENT_CHAR_PATTERN = re.compile(r'[\ufffd]{3,}')  # Multiple replacement chars
CONTROL_CHAR_HEAVY = re.compile(r'[\x00-\x1f]{3,}')

# CJK Unicode ranges for better international support
CJK_PATTERN = re.compile(r'[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]')

# Encoding normalization map
ENCODING_MAP = {
    'utf8': 'utf-8', 'windows1251': 'windows-1251', 'cp1251': 'windows-1251',
    'windows1252': 'windows-1252', 'cp1252': 'windows-1252',
    'iso88591': 'iso-8859-1', 'latin1': 'iso-8859-1',
    'koi8r': 'koi8-r', 'gb2312': 'gb2312', 'gbk': 'gbk', 'gb18030': 'gb18030',
    'big5': 'big5', 'eucjp': 'euc-jp', 'shiftjis': 'shift_jis', 'sjis': 'shift_jis',
    'euckr': 'euc-kr', 'windows1256': 'windows-1256', 'windows1255': 'windows-1255',
    'windows1254': 'windows-1254', 'windows1253': 'windows-1253', 'windows1250': 'windows-1250',
    'ascii': 'utf-8', 'usascii': 'utf-8',
}

FALLBACK_ENCODINGS = (
    'utf-8', 'windows-1252', 'iso-8859-1', 'windows-1251',
    'gb18030', 'shift_jis', 'euc-kr', 'big5', 'windows-1256',
    'iso-8859-15', 'koi8-r',
)



# DATA CLASSES

@dataclass(slots=True)
class ExtractedDocument:
    """Trafilatura-compatible document structure"""
    text: str
    title: Optional[str] = None
    author: Optional[str] = None
    hostname: Optional[str] = None
    url: str = ""
    date: Optional[str] = None
    sitename: Optional[str] = None
    description: Optional[str] = None
    categories: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    source: str = "warc_v4_processor"
    text_length: int = 0
    word_count: int = 0
    paragraph_count: int = 0
    heading_count: int = 0
    lines_removed: int = 0
    original_lines: int = 0
    fingerprint: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict, omitting None/empty values for cleaner output"""
        result = {
            'text': self.text,
            'url': self.url,
            'source': self.source,
            'text_length': self.text_length,
            'word_count': self.word_count,
        }
        
        if self.title:
            result['title'] = self.title
        if self.author:
            result['author'] = self.author
        if self.hostname:
            result['hostname'] = self.hostname
        if self.date:
            result['date'] = self.date
        if self.sitename:
            result['sitename'] = self.sitename
        if self.description:
            result['description'] = self.description
        if self.categories:
            result['categories'] = self.categories
        if self.tags:
            result['tags'] = self.tags
        if self.paragraph_count:
            result['paragraph_count'] = self.paragraph_count
        if self.heading_count:
            result['heading_count'] = self.heading_count
        if self.fingerprint:
            result['fingerprint'] = self.fingerprint
            
        return result



# WARC READER - Enhanced encoding handling
class WARCReader:
    """High-performance streaming WARC file reader with robust encoding"""
    
    __slots__ = ('warc_path', 'buffer_size')
    
    def __init__(self, warc_path: str, buffer_size: int = DEFAULT_BUFFER_SIZE):
        self.warc_path = Path(warc_path)
        self.buffer_size = buffer_size
        
    def _open_file(self):
        if self.warc_path.suffix == '.gz':
            return gzip.open(self.warc_path, 'rb')
        return open(self.warc_path, 'rb')
    
    def _read_headers(self, f) -> Optional[Dict[str, str]]:
        headers = {}
        while True:
            line = f.readline()
            if not line:
                return None
            line_str = line.decode('utf-8', errors='ignore').strip()
            if not line_str:
                break
            if line_str.startswith('WARC/'):
                headers['WARC-Version'] = line_str
            elif ':' in line_str:
                key, _, value = line_str.partition(':')
                headers[key.strip()] = value.strip()
        return headers
    
    def _read_http_response(self, f, content_length: int) -> Tuple[Dict[str, str], bytes]:
        content = f.read(content_length)
        try:
            # Find header/body separator
            header_end = content.find(b'\r\n\r\n')
            sep_len = 4
            if header_end == -1:
                header_end = content.find(b'\n\n')
                sep_len = 2
            
            if header_end != -1:
                http_headers_raw = content[:header_end].decode('utf-8', errors='ignore')
                body = content[header_end + sep_len:]
                http_headers = {}
                for line in http_headers_raw.split('\n'):
                    line = line.strip()
                    if ':' in line:
                        key, _, value = line.partition(':')
                        http_headers[key.strip().lower()] = value.strip()
                return http_headers, body
            return {}, content
        except Exception:
            return {}, content
    
    def _skip_trailing_newlines(self, f):
        while True:
            line = f.readline()
            if not line or line.strip():
                if line and line.strip():
                    try:
                        f.seek(-len(line), 1)
                    except Exception:
                        pass
                break
    
    @staticmethod
    def _normalize_encoding(encoding: str) -> str:
        """Normalize encoding names"""
        enc_key = encoding.lower().strip()
        enc_key = enc_key.replace('-', '').replace('_', '').replace('"', '').replace("'", '').replace(' ', '')
        return ENCODING_MAP.get(enc_key, encoding)
    
    def _detect_bom(self, body: bytes) -> Tuple[Optional[str], bytes]:
        """Detect and handle BOM (Byte Order Mark)"""
        if body.startswith(b'\xef\xbb\xbf'):
            return 'utf-8', body[3:]
        elif body.startswith(b'\xff\xfe\x00\x00'):
            return 'utf-32-le', body[4:]
        elif body.startswith(b'\x00\x00\xfe\xff'):
            return 'utf-32-be', body[4:]
        elif body.startswith(b'\xff\xfe'):
            return 'utf-16-le', body[2:]
        elif body.startswith(b'\xfe\xff'):
            return 'utf-16-be', body[2:]
        return None, body
    
    def _decode_html(self, body: bytes, http_headers: Dict[str, str]) -> str:
        """Decode HTML with robust multi-stage encoding detection"""
        
        # Stage 0: Check for BOM
        bom_encoding, body = self._detect_bom(body)
        if bom_encoding:
            try:
                return body.decode(bom_encoding)
            except (UnicodeDecodeError, LookupError):
                pass
        
        # Stage 1: HTTP Content-Type header
        charset = None
        content_type = http_headers.get('content-type', '')
        if 'charset=' in content_type.lower():
            match = HTTP_CHARSET_PATTERN.search(content_type)
            if match:
                charset = self._normalize_encoding(match.group(1))
        
        # Stage 2: HTML meta charset (first 4KB)
        if not charset:
            head = body[:4096]
            charset_match = CHARSET_PATTERN.search(head)
            if charset_match:
                try:
                    charset = self._normalize_encoding(
                        charset_match.group(1).decode('ascii', errors='ignore')
                    )
                except Exception:
                    pass
        
        # Stage 3: Try detected charset
        if charset:
            try:
                decoded = body.decode(charset)
                # Verify it decoded correctly (not too many replacement chars)
                if decoded.count('\ufffd') < len(decoded) * 0.05:  # <5% replacement
                    return decoded
            except (UnicodeDecodeError, LookupError):
                pass
        
        # Stage 4: Try common encodings
        for encoding in FALLBACK_ENCODINGS:
            try:
                decoded = body.decode(encoding)
                # Accept if fewer than 1% replacement characters
                if decoded.count('\ufffd') < len(decoded) * 0.01:
                    return decoded
            except (UnicodeDecodeError, LookupError):
                continue
        
        # Stage 5: UTF-8 with replacement (last resort)
        return body.decode('utf-8', errors='replace')
    
    def iterate_records(self) -> Iterator[Dict[str, Any]]:
        """Yield parsed WARC records with HTML content"""
        with self._open_file() as f:
            while True:
                warc_headers = self._read_headers(f)
                if warc_headers is None:
                    break
                
                content_length = int(warc_headers.get('Content-Length', 0))
                if content_length == 0:
                    continue
                
                warc_type = warc_headers.get('WARC-Type', '')
                
                if warc_type == 'response':
                    http_headers, body = self._read_http_response(f, content_length)
                    content_type = http_headers.get('content-type', '')
                    
                    # Accept HTML or unknown content types
                    if 'html' in content_type.lower() or not content_type:
                        try:
                            html = self._decode_html(body, http_headers)
                            yield {
                                'url': warc_headers.get('WARC-Target-URI', ''),
                                'html': html,
                                'warc_date': warc_headers.get('WARC-Date', ''),
                            }
                        except Exception:
                            pass
                else:
                    f.read(content_length)
                
                self._skip_trailing_newlines(f)



class MetadataExtractor:
    """Extract rich metadata from HTML"""
    
    __slots__ = ()
    
    DESCRIPTION_SELECTORS = (
        'meta[name="description"]', 'meta[property="og:description"]',
        'meta[name="twitter:description"]',
    )
    
    AUTHOR_SELECTORS = (
        'meta[name="author"]', 'meta[property="article:author"]',
        'meta[name="twitter:creator"]', '[rel="author"]', '[itemprop="author"]',
    )
    
    DATE_SELECTORS = (
        'meta[property="article:published_time"]', 'meta[name="date"]',
        'meta[name="DC.date"]', 'meta[property="og:updated_time"]',
        'time[datetime]', '[itemprop="datePublished"]',
    )
    
    SITENAME_SELECTORS = (
        'meta[property="og:site_name"]', 'meta[name="application-name"]',
        'meta[name="publisher"]',
    )
    
    KEYWORDS_SELECTORS = (
        'meta[name="keywords"]', 'meta[name="news_keywords"]',
    )
    
    def extract(self, parser: LexborHTMLParser, url: str) -> Dict[str, Any]:
        """Extract available metadata"""
        metadata = {}
        
        try:
            parsed = urlparse(url)
            metadata['hostname'] = parsed.netloc
        except Exception:
            metadata['hostname'] = None
        
        metadata['description'] = self._get_meta_content(parser, self.DESCRIPTION_SELECTORS)
        metadata['author'] = self._get_meta_content(parser, self.AUTHOR_SELECTORS)
        metadata['date'] = self._get_meta_content(parser, self.DATE_SELECTORS)
        metadata['sitename'] = self._get_meta_content(parser, self.SITENAME_SELECTORS)
        
        keywords = self._get_meta_content(parser, self.KEYWORDS_SELECTORS)
        metadata['tags'] = [k.strip() for k in keywords.split(',') if k.strip()][:10] if keywords else []
        metadata['categories'] = []
        
        return metadata
    
    def _get_meta_content(self, parser: LexborHTMLParser, selectors: Tuple[str, ...]) -> Optional[str]:
        """Get content from first matching selector"""
        for selector in selectors:
            try:
                node = parser.css_first(selector)
                if node:
                    content = node.attributes.get('content')
                    if content:
                        return content.strip()[:500]
                    dt = node.attributes.get('datetime')
                    if dt:
                        return dt.strip()[:100]
                    text = node.text(strip=True)
                    if text and len(text) < 500:
                        return text
            except Exception:
                continue
        return None


class LineFilter:
    """Filter individual lines while preserving document structure"""
    
    __slots__ = ('min_line_length', 'max_line_length', 'min_alpha_ratio')
    
    def __init__(
        self,
        min_line_length: int = 2,
        max_line_length: int = 15000,  # INCREASED for long paragraphs
        min_alpha_ratio: float = 0.20,  # LOWERED for higher retention
    ):
        self.min_line_length = min_line_length
        self.max_line_length = max_line_length
        self.min_alpha_ratio = min_alpha_ratio
    
    def _contains_copyright_symbol(self, line: str) -> bool:
        """Check if line contains copyright symbol © - NEW in v4"""
        return COPYRIGHT_LINE_PATTERN.search(line) is not None
    
    def _contains_ads_by_google(self, line: str) -> bool:
        """Check if line contains 'Ads by Google' - NEW in v4"""
        return ADS_BY_GOOGLE_PATTERN.search(line) is not None
    
    def _starts_with_post_comments(self, line: str) -> bool:
        """Check if line starts with 'Post Comments' - NEW in v4"""
        return POST_COMMENTS_PATTERN.match(line) is not None
    
    def _is_boilerplate_line(self, line: str) -> bool:
        """Check if line is boilerplate - EXPANDED in v4"""
        # Only check boilerplate patterns for SHORT lines (navigational)
        # Longer content is less likely to be boilerplate
        if len(line) > 80:
            return False
        
        # Common navigation/UI patterns
        if BOILERPLATE_LINE_PATTERNS.match(line):
            return True
        
        # Single word navigation items
        line_lower = line.lower().strip()
        nav_words = {'home', 'menu', 'search', 'login', 'register', 'contact', 
                     'about', 'faq', 'help', 'cart', 'checkout', 'settings',
                     'profile', 'notifications', 'messages', 'inbox', 'dashboard',
                     'admin', 'logout', 'signout', 'close', 'cancel', 'submit',
                     'save', 'delete', 'edit', 'update', 'create', 'new',
                     'previous', 'next', 'back', 'forward', 'refresh', 'reload'}
        if line_lower in nav_words:
            return True
        
        return False
    
    def _has_encoding_issues(self, line: str) -> bool:
        """Detect encoding problems in line"""
        # Multiple replacement characters
        if REPLACEMENT_CHAR_PATTERN.search(line):
            return True
        
        # Control characters
        if GARBAGE_PATTERN.search(line):
            return True
        
        # High ratio of non-printable characters
        printable = sum(1 for c in line if c.isprintable() or c in '\n\t ')
        if len(line) > 5 and printable / len(line) < 0.6:  # RELAXED from 0.7
            return True
        
        return False
    
    def _count_meaningful_chars(self, line: str) -> int:
        """Count alphabetic chars including CJK/international scripts"""
        count = 0
        for c in line:
            if c.isalpha():
                count += 1
            # CJK characters (Chinese, Japanese, Korean)
            elif '\u4e00' <= c <= '\u9fff':  # CJK Unified Ideographs
                count += 1
            elif '\u3040' <= c <= '\u30ff':  # Hiragana + Katakana
                count += 1
            elif '\uac00' <= c <= '\ud7af':  # Korean Hangul
                count += 1
            # Arabic
            elif '\u0600' <= c <= '\u06ff':
                count += 1
            # Hebrew
            elif '\u0590' <= c <= '\u05ff':
                count += 1
            # Devanagari (Hindi, Sanskrit, etc.)
            elif '\u0900' <= c <= '\u097f':
                count += 1
            # Thai
            elif '\u0e00' <= c <= '\u0e7f':
                count += 1
        return count
    
    def _is_valuable_line(self, line: str) -> bool:
        """Determine if a line contains valuable content - ENHANCED in v4"""
        line = line.strip()
        length = len(line)
        
        # Too short
        if length < self.min_line_length:
            return False
        
        # Too long (probably not structured content)
        if length > self.max_line_length:
            return False
        
        # Check for encoding issues
        if self._has_encoding_issues(line):
            return False
        
        # ===============
        # NEW v4 FILTERS - Remove specific lines
        # ===============
        
        # Remove lines containing copyright symbol ©
        if self._contains_copyright_symbol(line):
            return False
        
        # Remove lines containing "Ads by Google"
        if self._contains_ads_by_google(line):
            return False
        
        # Remove lines starting with "Post Comments"
        if self._starts_with_post_comments(line):
            return False
        
        # Very short lines need to be meaningful
        if length < 8:
            # Keep if it has any alphabetic or CJK character
            if self._count_meaningful_chars(line) == 0:
                return False
        
        # Check alphabetic ratio for longer lines (with international support)
        if length > 20:
            meaningful_count = self._count_meaningful_chars(line)
            meaningful_ratio = meaningful_count / length
            if meaningful_ratio < self.min_alpha_ratio:
                # Might be numbers/codes - check if it looks structured
                if not any(c in line for c in '.!?:;。！？：；'):
                    return False
        
        # Check for boilerplate (only for short lines)
        if self._is_boilerplate_line(line):
            return False
        
        return True
    
    def filter_lines(self, text: str) -> Tuple[str, int, int]:
        """
        Filter lines and return cleaned text with stats.
        Returns: (cleaned_text, lines_kept, lines_removed)
        """
        lines = text.split('\n')
        original_count = len(lines)
        
        filtered_lines = []
        for line in lines:
            # Normalize horizontal whitespace
            line = WHITESPACE_PATTERN.sub(' ', line).strip()
            
            if self._is_valuable_line(line):
                filtered_lines.append(line)
        
        lines_removed = original_count - len(filtered_lines)
        
        # Join and normalize multiple newlines
        cleaned = '\n'.join(filtered_lines)
        cleaned = MULTIPLE_NEWLINES.sub('\n\n', cleaned)
        
        return cleaned, len(filtered_lines), lines_removed



# CONTENT EXTRACTOR - Enhanced with expanded selectors & paragraph preservation
class ContentExtractor:
    """Extract and filter content from HTML with improved structure preservation"""
    
    __slots__ = ('metadata_extractor', 'line_filter', 'max_line_removal_ratio')
    
    REMOVE_TAGS: FrozenSet[str] = frozenset({
        'script', 'style', 'noscript', 'iframe', 'svg', 'canvas',
        'meta', 'link', 'head', 'nav', 'footer', 'aside',
        'form', 'input', 'button', 'select', 'textarea',
    })
    
    # EXPANDED MAIN CONTENT SELECTORS - Comprehensive coverage
    MAIN_SELECTORS = (
        # Semantic HTML5
        'article', 'main', '[role="main"]', '[role="article"]',
        
        # Common ID patterns
        '#content', '#main', '#article', '#post', '#story',
        '#main-content', '#page-content', '#primary', '#body-content',
        '#post-content', '#article-content', '#entry-content',
        '#blog-content', '#news-content', '#story-content',
        '#main-body', '#content-body', '#article-body',
        '#primary-content', '#main-article', '#single-content',
        
        # Common class patterns - Articles & Posts
        '.content', '.main', '.article', '.post-content', '.entry-content',
        '.post', '.entry', '.story', '.text', '.body', '.article-body',
        '.post-body', '.article-content', '.news-content', '.story-body',
        '.blog-post', '.blog-entry', '.blog-content', '.post-entry',
        '.single-post', '.single-content', '.single-entry',
        
        # WordPress specific
        '.wp-content', '.wp-block-post-content', '.entry-content',
        '.post-inner', '.post-article', '.hentry',
        
        # News & Magazine sites
        '.article-wrapper', '.article-text', '.article-main',
        '.news-article', '.news-body', '.news-text',
        '.story-wrapper', '.story-text', '.story-main',
        
        # CMS patterns
        '.node-content', '.field-body', '.field-content',
        '.content-area', '.content-wrapper', '.content-main',
        '.main-content-area', '.main-article', '.main-text',
        
        # Documentation & Prose
        '.prose', '.markdown-body', '.rich-text', '.text-content',
        '.doc-content', '.documentation', '.docs-content',
        '.page-content', '.page-body', '.page-text',
        
        # Forum & Discussion
        '.post-message', '.message-body', '.comment-content',
        '.thread-content', '.discussion-content',
        
        # E-commerce
        '.product-description', '.product-content', '.product-body',
        '.item-description', '.item-content',
        
        # Schema.org / Microdata selectors
        '[itemprop="articleBody"]', '[itemprop="text"]',
        '[itemprop="description"]', '[itemprop="mainContentOfPage"]',
        
        # Schema.org type selectors
        '[itemtype*="Article"]', '[itemtype*="BlogPosting"]',
        '[itemtype*="NewsArticle"]', '[itemtype*="WebPage"]',
        
        # Data-attribute patterns (modern frameworks)
        '[data-content="article"]', '[data-role="main"]',
        '[data-content-type="article"]', '[data-type="post"]',
        '[data-component="article"]', '[data-testid="article-body"]',
        
        # Section-based patterns
        'section.content', 'section.article', 'section.main',
        'div.content-area', 'div.article-area', 'div.main-area',
        
        # Generic deep patterns (lower priority)
        '.container .content', '.wrapper .content', '.inner .content',
    )
    
    def __init__(self, max_line_removal_ratio: float = DEFAULT_MAX_LINE_REMOVAL):
        self.metadata_extractor = MetadataExtractor()
        self.line_filter = LineFilter()
        self.max_line_removal_ratio = max_line_removal_ratio
    
    def _remove_boilerplate_elements(self, parser: LexborHTMLParser) -> None:
        """Remove script/style/navigation elements"""
        for tag in self.REMOVE_TAGS:
            for node in parser.css(tag):
                try:
                    node.decompose()
                except Exception:
                    pass
    
    def _get_main_node(self, parser: LexborHTMLParser) -> Optional[LexborNode]:
        """Find main content container with expanded selectors"""
        for selector in self.MAIN_SELECTORS:
            try:
                node = parser.css_first(selector)
                if node:
                    text = node.text(strip=True)
                    if len(text) > 20:  # LOWERED from 30 for higher retention
                        return node
            except Exception:
                continue
        return parser.body
    
    def _preserve_paragraph_structure(self, text: str) -> str:
        """
        IMPROVED: Preserve semantic paragraph structure while cleaning.
        Groups related content and maintains natural reading flow.
        """
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        paragraphs = []
        current_para_lines = []
        
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = WHITESPACE_PATTERN.sub(' ', line).strip()
            
            if not line:
                # Empty line - might be paragraph break
                if current_para_lines:
                    # Check if this looks like end of paragraph
                    last_line = current_para_lines[-1]
                    
                    # Sentence endings indicate paragraph break
                    if SENTENCE_END_PATTERN.search(last_line):
                        paragraphs.append(' '.join(current_para_lines))
                        current_para_lines = []
                    # If the accumulated text is substantial, also break
                    elif len(' '.join(current_para_lines)) > 200:
                        paragraphs.append(' '.join(current_para_lines))
                        current_para_lines = []
                    # Otherwise, this empty line might just be HTML artifact - continue accumulating
                continue
            
            # Check if this line should start a new paragraph
            should_break = False
            
            # Lines that look like headings (short, no sentence ending)
            if len(line) < 100 and not SENTENCE_END_PATTERN.search(line):
                # Check if previous accumulated content is substantial
                if current_para_lines and len(' '.join(current_para_lines)) > 50:
                    # This might be a heading for next section
                    if line[0].isupper() or (line[0].isdigit() and '.' in line[:5]):
                        should_break = True
            
            # Bullet points or list items start new logical units
            if line.startswith(('•', '-', '*', '–', '—', '►', '●')) or \
               (len(line) > 2 and line[0].isdigit() and line[1] in '.):'):
                should_break = True
            
            if should_break and current_para_lines:
                paragraphs.append(' '.join(current_para_lines))
                current_para_lines = []
            
            current_para_lines.append(line)
        
        # Don't forget the last paragraph
        if current_para_lines:
            paragraphs.append(' '.join(current_para_lines))
        
        # Filter out very short paragraphs that are likely noise
        # But be lenient - short paragraphs can be valid
        filtered_paragraphs = []
        for para in paragraphs:
            para = para.strip()
            if len(para) >= 3:  # Very lenient - keep almost everything
                filtered_paragraphs.append(para)
        
        # Join with double newlines for clear paragraph separation
        return '\n\n'.join(filtered_paragraphs)
    
    def _count_structure(self, parser: LexborHTMLParser) -> Tuple[int, int]:
        """Count paragraphs and headings"""
        try:
            paragraphs = len(parser.css('p'))
            headings = len(parser.css('h1, h2, h3, h4, h5, h6'))
            return paragraphs, headings
        except Exception:
            return 0, 0
    
    def extract(self, html: str, url: str) -> Optional[ExtractedDocument]:
        """Extract, filter, and return document with preserved structure"""
        try:
            parser = LexborHTMLParser(html)
        except Exception:
            return None
        
        # Remove boilerplate elements
        self._remove_boilerplate_elements(parser)
        
        # Get title
        title_node = parser.css_first('title')
        title = title_node.text().strip()[:500] if title_node else None
        
        # Get metadata
        metadata = self.metadata_extractor.extract(parser, url)
        
        # Get structure counts
        para_count, heading_count = self._count_structure(parser)
        
        # Get main content
        main_node = self._get_main_node(parser)
        if not main_node:
            return None
        
        # Extract raw text with newline separators
        raw_text = main_node.text(deep=True, separator='\n', strip=True)
        
        # IMPROVED: Preserve paragraph structure
        normalized_text = self._preserve_paragraph_structure(raw_text)
        
        if not normalized_text:
            return None
        
        # Apply line-level filtering
        filtered_text, lines_kept, lines_removed = self.line_filter.filter_lines(normalized_text)
        
        if not filtered_text:
            return None
        
        # Check if too many lines were removed (document is mostly boilerplate)
        total_lines = lines_kept + lines_removed
        if total_lines > 0:
            removal_ratio = lines_removed / total_lines
            if removal_ratio > self.max_line_removal_ratio:
                return None
        
        # Compute final metrics
        words = WORD_PATTERN.findall(filtered_text)
        word_count = len(words)
        text_length = len(filtered_text)
        
        # Create fingerprint
        fingerprint = hashlib.md5(
            filtered_text[:1000].encode('utf-8', errors='ignore')
        ).hexdigest()[:16]
        
        return ExtractedDocument(
            text=filtered_text,
            title=title,
            author=metadata.get('author'),
            hostname=metadata.get('hostname'),
            url=url,
            date=metadata.get('date'),
            sitename=metadata.get('sitename'),
            description=metadata.get('description'),
            categories=metadata.get('categories', []),
            tags=metadata.get('tags', []),
            text_length=text_length,
            word_count=word_count,
            paragraph_count=para_count,
            heading_count=heading_count,
            lines_removed=lines_removed,
            original_lines=total_lines,
            fingerprint=fingerprint,
        )


# QUALITY FILTER - Document level (RELAXED for higher retention)

class QualityFilter:
    """Document-level quality filter (runs after line filtering)"""
    
    __slots__ = ('min_length', 'max_length', 'min_words', 'min_unique_ratio')
    
    ERROR_PATTERNS = frozenset({
        'page not found', '404', 'error 404', 'access denied', '403',
        'internal server error', '500', 'service unavailable', '503',
        'bad gateway', '502', 'coming soon', 'under construction',
    })
    
    # Bot-check/verification page patterns - remove entire document
    BOT_CHECK_PATTERNS = frozenset({
        'please wait while your request is being verified',
        'checking your browser before accessing',
        'please enable cookies',
        'just a moment',
        'ddos protection by',
        'attention required',
        'please complete the security check',
        'ray id:',
    })
    
    def __init__(
        self,
        min_length: int = DEFAULT_MIN_LENGTH,
        max_length: int = DEFAULT_MAX_LENGTH,
        min_words: int = DEFAULT_MIN_WORDS,
        min_unique_ratio: float = 0.08,  # LOWERED from 0.12 for higher retention
        **kwargs
    ):
        self.min_length = min_length
        self.max_length = max_length
        self.min_words = min_words
        self.min_unique_ratio = min_unique_ratio
    
    def _is_error_page(self, text: str) -> bool:
        """Detect error pages"""
        if len(text) > 500:
            return False
        text_lower = text.lower()
        return any(e in text_lower for e in self.ERROR_PATTERNS)
    
    def _is_bot_check_page(self, text: str) -> bool:
        """Detect bot-check/verification pages (Cloudflare, etc.) - remove entire doc"""
        text_lower = text.lower()
        return any(pattern in text_lower for pattern in self.BOT_CHECK_PATTERNS)
    
    def _is_spam(self, text: str, word_count: int) -> bool:
        """Detect spam content - RELAXED"""
        if word_count < 50:  # INCREASED from 30 - be more lenient with short content
            return False
        
        words = WORD_PATTERN.findall(text.lower())
        unique_ratio = len(set(words)) / max(len(words), 1)
        
        if unique_ratio < self.min_unique_ratio:
            return True
        
        # Excessive commas - RELAXED threshold
        if text.count(',') / max(len(text), 1) > 0.10:  # INCREASED from 0.08
            return True
        
        return False
    
    def passes(self, doc: ExtractedDocument) -> Tuple[bool, str]:
        """Check if document passes quality filters"""
        
        if doc.text_length < self.min_length:
            return False, "too_short"
        
        if doc.text_length > self.max_length:
            return False, "too_long"
        
        if doc.word_count < self.min_words:
            return False, "few_words"
        
        if self._is_error_page(doc.text):
            return False, "error_page"
        
        if self._is_bot_check_page(doc.text):
            return False, "bot_check_page"
        
        if self._is_spam(doc.text, doc.word_count):
            return False, "spam"
        
        return True, ""



# FILE PROCESSOR

def process_single_warc(
    warc_path: str,
    output_path: str,
    filter_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Process a single WARC file"""
    
    max_line_removal = filter_config.pop('max_line_removal', DEFAULT_MAX_LINE_REMOVAL)
    extractor = ContentExtractor(max_line_removal_ratio=max_line_removal)
    quality_filter = QualityFilter(**filter_config)
    
    stats = {
        'warc_file': Path(warc_path).name,
        'total_records': 0,
        'passed': 0,
        'failed': 0,
        'total_lines_removed': 0,
        'filter_reasons': {}
    }
    
    write_buffer = deque()
    batch_size = DEFAULT_BATCH_SIZE
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    start_time = time.time()
    
    with open(output_path, 'w', encoding='utf-8', buffering=2*1024*1024) as out_file:
        reader = WARCReader(warc_path)
        
        for record in reader.iterate_records():
            stats['total_records'] += 1
            
            try:
                doc = extractor.extract(record['html'], record['url'])
                
                if not doc:
                    stats['failed'] += 1
                    stats['filter_reasons']['empty'] = stats['filter_reasons'].get('empty', 0) + 1
                    continue
                
                # Track lines removed
                stats['total_lines_removed'] += doc.lines_removed
                
                passes, reason = quality_filter.passes(doc)
                
                if passes:
                    stats['passed'] += 1
                    write_buffer.append(json.dumps(doc.to_dict(), ensure_ascii=False))
                    
                    if len(write_buffer) >= batch_size:
                        out_file.write('\n'.join(write_buffer) + '\n')
                        write_buffer.clear()
                else:
                    stats['failed'] += 1
                    stats['filter_reasons'][reason] = stats['filter_reasons'].get(reason, 0) + 1
                        
            except Exception as e:
                stats['failed'] += 1
                stats['filter_reasons']['error'] = stats['filter_reasons'].get('error', 0) + 1
        
        if write_buffer:
            out_file.write('\n'.join(write_buffer) + '\n')
    
    stats['time_seconds'] = round(time.time() - start_time, 2)
    return stats



# BATCH PROCESSOR

class WARCBatchProcessor:
    """Process multiple WARC files in parallel"""
    
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        num_workers: int = None,
        **filter_kwargs
    ):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.num_workers = num_workers or max(1, multiprocessing.cpu_count() - 1)
        self.filter_config = filter_kwargs
        
        self.warc_files = list(self.input_dir.glob('*.warc.gz')) + list(self.input_dir.glob('*.warc'))
        
        self.global_stats = {
            'total_files': len(self.warc_files),
            'files_processed': 0,
            'total_records': 0,
            'total_passed': 0,
            'total_failed': 0,
            'total_lines_removed': 0,
            'all_filter_reasons': {}
        }
    
    def process(self) -> Dict[str, Any]:
        """Process all WARC files"""
        if not self.warc_files:
            print(f"No WARC files found in {self.input_dir}")
            return self.global_stats
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._print_header()
        
        start_time = time.time()
        
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {}
            
            for warc_file in self.warc_files:
                output_file = self.output_dir / f"{warc_file.stem}.jsonl"
                future = executor.submit(
                    process_single_warc,
                    str(warc_file),
                    str(output_file),
                    self.filter_config.copy()
                )
                futures[future] = warc_file.name
            
            for future in as_completed(futures):
                warc_name = futures[future]
                try:
                    stats = future.result()
                    self._update_stats(stats)
                    self._print_progress(warc_name, stats)
                except Exception as e:
                    print(f"✗ {warc_name}: Error - {e}")
        
        self.global_stats['total_time'] = round(time.time() - start_time, 2)
        self._print_summary()
        
        return self.global_stats
    
    def _print_header(self):
        print("=" * 70)
        print("WARC Processor v4 - Enhanced Line Filtering & Expanded Boilerplate")
        print("=" * 70)
        print(f"Input:   {self.input_dir}")
        print(f"Output:  {self.output_dir}")
        print(f"Files:   {len(self.warc_files)}")
        print(f"Workers: {self.num_workers}")
        print("-" * 70)
        print("Features:")
        print("  ✓ Line-level filtering (remove bad lines, keep documents)")
        print("  ✓ NEW: Copyright (©) line removal")
        print("  ✓ NEW: 'Ads by Google' line removal")
        print("  ✓ NEW: 'Post Comments' line removal")
        print("  ✓ EXPANDED boilerplate patterns (150+ patterns)")
        print("  ✓ Enhanced international/CJK support")
        print("  ✓ RELAXED filters for higher data retention")
        print("-" * 70)
    
    def _update_stats(self, stats: Dict[str, Any]):
        self.global_stats['files_processed'] += 1
        self.global_stats['total_records'] += stats['total_records']
        self.global_stats['total_passed'] += stats['passed']
        self.global_stats['total_failed'] += stats['failed']
        self.global_stats['total_lines_removed'] += stats.get('total_lines_removed', 0)
        
        for reason, count in stats['filter_reasons'].items():
            self.global_stats['all_filter_reasons'][reason] = \
                self.global_stats['all_filter_reasons'].get(reason, 0) + count
    
    def _print_progress(self, warc_name: str, stats: Dict[str, Any]):
        pass_rate = (stats['passed'] / max(stats['total_records'], 1)) * 100
        print(f"✓ {warc_name}: {stats['passed']}/{stats['total_records']} ({pass_rate:.0f}%) in {stats['time_seconds']}s")
    

    def _print_summary(self):
        print("\n" + "=" * 70)
        print("BATCH PROCESSING COMPLETE")
        print("=" * 70)
        
        stats = self.global_stats
        print(f"\nFiles Processed: {stats['files_processed']}/{stats['total_files']}")
        print(f"Total Time: {stats['total_time']}s")
        print(f"Total Records: {stats['total_records']}")
        print(f"Passed Filters: {stats['total_passed']}")
        print(f"Failed Filters: {stats['total_failed']}")
        print(f"Lines Cleaned: {stats['total_lines_removed']:,}")
        
        if stats['total_records'] > 0:
            pass_rate = (stats['total_passed'] / stats['total_records']) * 100
            throughput = stats['total_records'] / max(stats['total_time'], 0.001)
            avg_per_warc = stats['total_passed'] // max(stats['files_processed'], 1)
            print(f"Pass Rate: {pass_rate:.1f}%")
            print(f"Throughput: {throughput:.1f} records/sec")
            print(f"Avg docs/WARC: {avg_per_warc}")
        
        if stats['all_filter_reasons']:
            print("\n--- Filter Failure Breakdown ---")
            for reason, count in sorted(stats['all_filter_reasons'].items(), key=lambda x: x[1], reverse=True):
                pct = (count / max(stats['total_failed'], 1)) * 100
                print(f"  {reason}: {count} ({pct:.1f}%)")



# MAIN

def main():
    parser = argparse.ArgumentParser(
        description='WARC Processor v4 - Enhanced Line Filtering & Expanded Boilerplate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python WARC_version_4.py /path/to/warcs -o ./outputs
  python WARC_version_4.py ./warcs -o ./out --workers 4
  python WARC_version_4.py ./warcs -o ./out --max-line-removal 0.85
        """
    )
    
    parser.add_argument('input_dir', help='Folder containing WARC files')
    parser.add_argument('-o', '--output', required=True, help='Output folder')
    parser.add_argument('--workers', type=int, default=None, help='Parallel workers')
    parser.add_argument('--min-length', type=int, default=DEFAULT_MIN_LENGTH,
                        help=f'Minimum text length (default: {DEFAULT_MIN_LENGTH})')
    parser.add_argument('--min-words', type=int, default=DEFAULT_MIN_WORDS,
                        help=f'Minimum word count (default: {DEFAULT_MIN_WORDS})')
    parser.add_argument('--max-line-removal', type=float, default=DEFAULT_MAX_LINE_REMOVAL,
                        help=f'Max ratio of lines to remove before discarding doc (default: {DEFAULT_MAX_LINE_REMOVAL})')
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}", file=sys.stderr)
        sys.exit(1)
    
    try:
        processor = WARCBatchProcessor(
            input_dir=str(input_dir),
            output_dir=args.output,
            num_workers=args.workers,
            min_length=args.min_length,
            min_words=args.min_words,
            max_line_removal=args.max_line_removal,
        )
        processor.process()
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
