#!/usr/bin/env python
# -*- coding:utf-8 -*-

import urlparse, logging, os.path, datetime, time, imghdr
from lib import feedparser
from bs4 import BeautifulSoup, Comment
from calibre.utils.img import rescale_image
from config import *
from lib.urlopener import URLOpener

class BaseMagazine(object):

    title         = ''
    description   = ''
    mastheadfile  = DEFAULT_MASTHEAD
    coverfile     = DEFAULT_COVER
    deliver_times = []
    deliver_days  = []
    setting       = {}
    book_list     = []

    def Items(self, opts = None, user = None):
        i = 0
        for book_class in self.book_list:
            book = book_class(imgindex = i, setting = self.setting)
            for data in book.Items(opts,user): yield data
            i = book.imgindex

    __author__ = ''; max_articles_per_feed = 30; oldest_article = 1
    host = None; network_timeout = None; fetch_img_via_ssl = False
    language = 'zh-cn'; extra_header = {}; feed_encoding = page_encoding = "utf-8"
    keep_image = True; fulltext_by_readability = True; fulltext_by_instapaper = False
    needs_subscription = False; login_url = ''; account = ''
    password = ''; form_4_login = None; keep_only_tags = []
    remove_tags_after = []; remove_tags_before = []; insta_remove_tags = []
    insta_remove_attrs = []; insta_remove_classes = []; insta_remove_ids = []
    positive_classes = []; img_min_size = 1024; remove_tags = []
    remove_ids = []; remove_classes = []; remove_attrs = []
    extra_css = ''; url_filters = []; feeds = []
    def __init__(self, log=None, imgindex=0): pass

class BaseFeedBook(object):

    feeds = []
    setting = {}

    def __init__(self, imgindex = 0, setting = {}):

        default = {}
        default["oldest_article"] = 0          # 最旧文章
        default["max_article"]    = -1         # 最多抓取的文章个数
        default["deliver_days"]   = 0          # 每隔几天投递一次
        default["timeout"]        = 30         # 超时时间修正
        default["headers"]        = {}         # 请求头设置
        default["retry_time"]     = 1          # 获取 Feed 失败时的重试次数
        default["catch"]          = []         # 待提取的内容
        default["remove_tags"]    = []         # 需移除的标签
        default["remove_ids"]     = []         # 需移除的 id
        default["remove_classes"] = []         # 需移除的 class
        default["keep_image"]     = True       # 是否保留图片
        default["img_file_size"]  = 1024       # 图片文件的最小大小
        default["img_size"]       = (600,800)  # 图片缩放后的大小

        default.update(setting)
        default.update(self.setting)
        if default["deliver_days"] > 0:
            default["oldest_article"] = default["deliver_days"]

        self.setting = default
        self._imgindex = imgindex

    @property
    def imgindex(self):
        self._imgindex += 1
        return self._imgindex

    @property
    def timeout(self):
        CONNECTION_TIMEOUT + self.setting["timeout"]

    @classmethod
    def urljoin(self, base, url):
        join = urlparse.urljoin(base,url)
        url = urlparse.urlsplit(join)
        path = os.path.normpath(url.path)
        return urlparse.urlunsplit((url.scheme, url.netloc, path, url.query, url.fragment))

    def frag_to_xhtml(self, content, title):
        if content.find('<html') > 0: return content
        frame = [
          '<!DOCTYPE html SYSTEM "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">',
          '<html xmlns="http://www.w3.org/1999/xhtml">',
          '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8">',
          '<title>%s</title></head><body><p>%s</p></body></html>'
        ]
        frame = ''.join(frame)
        return frame % (title, content)

    def featch_content(self, opener, url):
        retry_time = 0
        while retry_time < self.setting["retry_time"]:
            result = opener.open(url)
            if result.status_code == 200 and result.content: return result.content
            else:
                retry_time += 1
                text = 'Fetch content failed(%s), retry after 30s second(%s)'
                logging.warn( text % (result.status_code, retry_time) )
                time.sleep(30)
        return None

    def parse_feed_urls(self):
        # return [(section,title,url,desc),..]
        urls = []
        time = datetime.datetime.now()
        for feed in self.feeds:
            section, url = feed[0], feed[1]
            if self.check_feed_skip(time):
                logging.info( "Skip feed: %s" % (section) )
                continue
            opener = URLOpener(timeout = self.timeout, headers = self.setting["headers"])
            content = self.featch_content(opener, url)
            if content:
                try: content = content.decode('utf-8')
                except UnicodeDecodeError:
                    content = AutoDecoder(True).decode(result.content,opener.realurl,result.headers)
                content = feedparser.parse(content)
                for title, urlfeed, description in self.parser_feed(time, content):
                    urls.append( (section, title, urlfeed, description) )
            else: logging.warn('Fetch feed failed, skip(%s)' % section)
        return urls

    def check_feed_skip(self, time):
        if self.setting["deliver_days"] < 1: return False
        days = (time - datetime.datetime(2016,1,1)).days
        return days % self.setting["deliver_days"] != 0

    def parser_feed(self, time, feed):
        for e in feed['entries'][:self.setting["max_article"]]:
            if self.check_article_skip(time, e): return
            if hasattr(e, 'link'): urlfeed = e.link
            else: continue
            desc = None
            summary = e.summary if hasattr(e, 'summary') else ""
            content = e.content[0]['value'] if (hasattr(e, 'content') and e.content[0]['value']) else ""
            desc = content if len(content) > len(summary) else summary
            yield (e.title, urlfeed, desc)

    def check_article_skip(self, time, e):
        if self.setting["oldest_article"] < 1: return False
        updated = None
        if hasattr(e, 'updated_parsed') and e.updated_parsed: updated = e.updated_parsed
        elif hasattr(e, 'published_parsed') and e.published_parsed: updated = e.published_parsed
        elif hasattr(e, 'created_parsed'): updated = e.created_parsed
        if updated is None: return False
        updated = datetime.datetime(*(updated[0:6]))
        days = (time - updated).days
        return days > self.setting["oldest_article"]

    def Items(self, opts = None, user = None):
        for section, ftitle, url, desc in self.parse_feed_urls():
            article = self.frag_to_xhtml(desc, ftitle)
            soup = BeautifulSoup(article, "lxml")
            self.process_article(soup, url)

            thumbnail = None
            if self.setting["keep_image"]:
                for imgmime, imgurl, fnimg, imgcontent in self.process_image(soup, url):
                    if thumbnail: yield (imgmime, imgurl, fnimg, imgcontent, None, None)
                    else:
                        thumbnail = imgurl
                        yield (imgmime, imgurl, fnimg, imgcontent, None, True)

            content = unicode(soup)
            brief = u''
            if GENERATE_TOC_DESC:
                body = soup.find('body')
                for h in body.find_all(['h1','h2']): h.decompose()
                for s in body.stripped_strings:
                    brief += unicode(s) + u' '
                    if len(brief) >= TOC_DESC_WORD_LIMIT:
                        brief = brief[:TOC_DESC_WORD_LIMIT]
                        break
            yield (section, url, ftitle, content, brief, thumbnail)

    def process_article(self, soup, url):
        self.clear_article(soup)
        for attr in [attr for attr in soup.html.body.attrs]: del body[attr]
        for x in soup.find_all(['article', 'aside', 'header', 'footer', 'nav',
            'figcaption', 'figure', 'section', 'time']):
            x.name = 'div'

    def clear_article(self, soup):
        if self.setting["catch"]:
            body = soup.new_tag('body')
            try:
                for spec in self.setting["catch"]:
                    for tag in soup.find('body').find_all(**spec):
                        body.insert(len(body.contents), tag)
                soup.find('body').replace_with(body)
            except: pass

        remove_tags = ['script','object','video','embed','noscript','style','link']
        remove_classes = []
        remove_ids = ['controlbar_container']
        remove_attrs = ['width','height','onclick','onload','style']

        remove_tags += self.setting["remove_tags"]
        remove_classes += self.setting["remove_classes"]
        remove_ids += self.setting["remove_ids"]

        if not self.setting["keep_image"]: remove_tags += ['img']

        for tag in soup.find_all(remove_tags):
          tag.decompose()
        for id in remove_ids:
          for tag in soup.find_all(attrs={"id":id}): tag.decompose()
        for cls in remove_classes:
            for tag in soup.find_all(attrs={"class":cls}): tag.decompose()
        for attr in remove_attrs:
            for tag in soup.find_all(attrs={attr:True}): del tag[attr]
        for cmt in soup.find_all(text=lambda text:isinstance(text, Comment)):
            cmt.extract()

        if not self.setting["keep_image"]: return
        for img in soup.find_all('img'):
            if img.parent and img.parent.parent and img.parent.name == 'a':
                img.parent.replace_with(img)

    def process_image(self, soup, url):
        opener = URLOpener(timeout = self.timeout, headers = self.setting["headers"])
        for imgurl, img in self.process_image_url(soup, url):
            imgcontent = self.featch_content(opener, imgurl)
            if (not imgcontent) or (len(imgcontent) < self.setting["img_file_size"]):
                img.decompose()
                continue
            imgcontent = self.edit_image(imgcontent)
            imgtype = imghdr.what(None, imgcontent)
            if not imgtype:
                img.decompose()
                continue
            fnimg = "img%d.%s" % (self.imgindex, 'jpg' if imgtype=='jpeg' else imgtype)
            img['src'] = fnimg
            yield (r"image/" + imgtype, imgurl, fnimg, imgcontent)

    def process_image_url(self, soup, url):
        for img in soup.find_all('img'):
            imgurl = img['src'] if 'src' in img.attrs else ''
            if imgurl:
                if not imgurl.startswith('http'): imgurl = self.urljoin(url, imgurl)
                yield (imgurl, img)
            else:
                img.decompose()
                continue

    def edit_image(self, data):
        try: return rescale_image(data, reduceto = self.setting["img_size"])
        except Exception as e:
            logging.warn('Process image failed (%s), use original image.' % str(e))
            return data
