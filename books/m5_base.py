#!/usr/bin/env python
# -*- coding:utf-8 -*-

import urlparse, os.path, datetime, time, imghdr, random, urllib
from lib import feedparser
from bs4 import BeautifulSoup, Comment
from calibre.utils.img import rescale_image
from config import *
from lib.urlopener import URLOpener
from lib.readability import readability


# 书籍与杂志的共通父类，定义与原版相同的对外接口以防止出错
class Base(object):

    title = ''; description = ''; mastheadfile = DEFAULT_MASTHEAD
    coverfile = DEFAULT_COVER; deliver_times = []; deliver_days = []
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


# 书籍的父类，定义了书籍处理的共通方法
class BaseBook(Base):

    setting = {}

    # 网络连接的默认设置
    setting["timeout"]        = 30         # 超时时间修正
    setting["headers"]        = {}         # 请求头设置
    setting["host"]           = None       # Host 设置
    setting["retry_time"]     = 1          # 联网失败时的重试次数
    setting["retry_sleep"]    = 30         # 联网失败时的重试延迟时间
    setting["proxy_content"]  = False      # 通过转发服务器获取内容
    setting["proxy_img"]      = False      # 通过转发服务器获取图片

    # 内容提取的默认设置
    setting["add_share"]      = True       # 是否在文章末尾添加分享链接
    setting["keep_image"]     = True       # 是否保留图片
    setting["img_file_size"]  = 1024       # 图片文件的最小字节
    setting["img_size"]       = (600,800)  # 图片缩放后的大小
    setting["block_img"]      = []         # 图片地址中包含指定文字时，放弃获取该图片
    setting["readability"]    = False      # 使用 readability 自动提取正文内容
    setting["catch"]          = []         # 待提取的内容
    setting["remove"]         = []         # 待移除的内容
    setting["remove_tags"]    = []         # 需移除的标签
    setting["remove_ids"]     = []         # 需移除的 id
    setting["remove_classes"] = []         # 需移除的 class

    # 针对 Feed 的默认设置
    setting["oldest_article"] = 1          # 最旧文章
    setting["max_article"]    = -1         # 最多抓取的文章个数
    setting["deliver_days"]   = 0          # 每隔几天投递一次

    def __init__(self, log = None, imgindex = 0, setting = {}):

        default = {}
        default["timeout"]        = 30
        default["headers"]        = {}
        default["host"]           = None
        default["retry_time"]     = 1
        default["retry_sleep"]    = 30
        default["proxy_content"]  = False
        default["proxy_img"]      = False
        default["add_share"]      = True
        default["keep_image"]     = True
        default["img_file_size"]  = 1024
        default["img_size"]       = (600,800)
        default["block_img"]      = []
        default["readability"]    = False
        default["catch"]          = []
        default["remove"]         = []
        default["remove_tags"]    = []
        default["remove_ids"]     = []
        default["remove_classes"] = []
        default["oldest_article"] = 1
        default["max_article"]    = -1
        default["deliver_days"]   = 0

        default.update(setting)
        default.update(self.setting)
        if default["deliver_days"] > 0:
            default["oldest_article"] = default["deliver_days"]

        self.setting = default
        self._imgindex = imgindex

        self.log = default_log if log is None else log
        self.opener = URLOpener(self.setting['host'], timeout = CONNECTION_TIMEOUT + self.setting["timeout"],
          headers = self.setting["headers"])

    @property
    def imgindex(self):
        self._imgindex += 1
        return self._imgindex

    def urljoin(self, base, url):
        join = urlparse.urljoin(base,url)
        url = urlparse.urlsplit(join)
        path = os.path.normpath(url.path)
        return urlparse.urlunsplit((url.scheme, url.netloc, path, url.query, url.fragment))

    def featch_url(self, url, retry = None):
        if not retry: retry = self.setting["retry_time"]
        retry_time = 0
        while retry_time < retry:
            result = self.opener.open(url)
            if result.status_code == 200: return result.content
            else:
                retry_time += 1
                sleep_time = self.setting["retry_sleep"]
                sleep_time = random.randint(sleep_time, sleep_time + 30)
                text = 'Fetch content failed(%s):%s, retry after %s second(%s)'
                self.log.warn( text % (url, URLOpener.CodeMap(result.status_code), sleep_time, retry_time) )
                time.sleep(sleep_time)
        self.log.warn('Fail!')
        return None

    def featch_content(self, url):
        if self.setting['proxy_content']: url = SHARE_FUCK_GFW_SRV % urllib.quote(url)
        content = self.featch_url(url)
        if content: content = content.decode("utf-8")
        return content

    def featch_img_content(self, url):
        if self.check_url_block(self.setting["block_img"], url):
            self.log.warn('Customize block img(%s)' % url)
            return None
        if self.setting['proxy_img']: url = SHARE_FUCK_GFW_SRV % urllib.quote(url)
        return self.featch_url(url)

    def check_url_block(self, rule, url):
        for key_word in rule:
            if key_word in url: return True
        return False

    def get_items(self):
        # yield (section, title, url, soup, brief)
        pass

    def Items(self, opts = None, user = None):
        for section, title, url, soup, brief in self.get_items():
            thumbnail = None
            for imgmime, imgurl, fnimg, imgcontent in self.process_image(soup, url):
                if thumbnail: yield (imgmime, imgurl, fnimg, imgcontent, None, None)
                else:
                    thumbnail = imgurl
                    yield (imgmime, imgurl, fnimg, imgcontent, None, True)
            if self.setting["add_share"]: soup = self.add_share_link(soup, url = url, title = title)
            content = unicode(soup)
            if brief is None: brief = self.generate_brief(soup)
            yield (section, url, title, content, brief, thumbnail)

    def add_share_link(self, soup, url, title):
        h3 = soup.new_tag('h3')
        h3.append(u'分享文章')
        soup.body.append(h3)
        link = []
        quote_url = urllib.quote(url).encode('utf-8')
        href = url
        link.append((href, u'原文地址'))
        href = u'http://note.youdao.com/memory/?url=%s&title=%s&sumary=&product=' % (quote_url, title)
        link.append((href, u'分享到有道云笔记'))
        for href, text in link:
            div = soup.new_tag('div')
            a = soup.new_tag('a', href = href)
            a.append(text)
            div.append(a)
            soup.body.append(div)
        return soup

    def generate_brief(self, soup):
        brief = u''
        if not GENERATE_TOC_DESC: return brief
        body = soup.find('body')
        for h in body.find_all(['h1','h2']): h.decompose()
        for s in body.stripped_strings:
            brief += unicode(s) + u' '
            if len(brief) >= TOC_DESC_WORD_LIMIT:
                brief = brief[:TOC_DESC_WORD_LIMIT]
                break
        return brief

    def process_article(self, html):
        soup = self.clear_article(html)
        for attr in [attr for attr in soup.html.body.attrs]: del body[attr]
        for x in soup.find_all(['article', 'aside', 'header', 'footer', 'nav',
            'figcaption', 'figure', 'section', 'time']):
            x.name = 'div'
        return soup

    def clear_article(self, html):
        if self.setting["readability"]:
            doc = readability.Document(html)
            soup = BeautifulSoup(doc.summary(html_partial = False), "lxml")
            if len(soup.find('body').contents) > 0: return soup
        return self.clear_article_by_soup(html)

    def clear_article_by_soup(self, html):
        soup = BeautifulSoup(html, "lxml")

        if self.setting["catch"]:
            body = soup.new_tag('body')
            try:
                for spec in self.setting["catch"]:
                    for tag in soup.find_all(**spec):
                        body.insert(len(body.contents), tag)
                soup.find('body').replace_with(body)
            except:
                self.log.warn("catch contents failed...")
                debug_mail(html)

        for spec in self.setting["remove"]:
            for tag in soup.find_all(**spec): tag.decompose()

        remove_tags = ['script','object','video','embed','noscript','style','link']
        remove_classes = []
        remove_ids = ['controlbar_container']
        remove_attrs = ['width','height','onclick','onload','style','id']
        remove_tags += self.setting["remove_tags"]
        remove_classes += self.setting["remove_classes"]
        remove_ids += self.setting["remove_ids"]
        for tag in soup.find_all(remove_tags): tag.decompose()
        for id in remove_ids:
            for tag in soup.find_all(attrs={"id":id}): tag.decompose()
        for cls in remove_classes:
            for tag in soup.find_all(attrs={"class":cls}): tag.decompose()
        for attr in remove_attrs:
            for tag in soup.find_all(attrs={attr:True}): del tag[attr]
        for cmt in soup.find_all(text=lambda text:isinstance(text, Comment)):
            cmt.extract()

        return soup

    def process_image(self, soup, url):
        for img in soup.find_all('img'):
            if self.setting["keep_image"]:
                if img.parent and img.parent.parent and img.parent.name == 'a':
                    img.parent.replace_with(img)
            else: img.decompose()
        if not self.setting["keep_image"]: return
        for imgurl, img in self.process_image_url(soup, url):
            imgcontent = self.featch_img_content(imgurl)
            if (not imgcontent) or (len(imgcontent) < self.setting["img_file_size"]):
                img.decompose()
                continue
            imgcontent = self.edit_image(imgcontent, imgurl)
            imgtype = imghdr.what(None, imgcontent)
            if not imgtype:
                img.decompose()
                continue
            fnimg = "img%d.%s" % (self.imgindex, 'jpg' if imgtype=='jpeg' else imgtype)
            img['src'] = fnimg
            yield (r"image/" + imgtype, imgurl, fnimg, imgcontent)

    def process_image_url(self, soup, url):
        for img in soup.find_all('img'):
            imgurl = self.get_image_url(img)
            if imgurl:
                if imgurl.startswith('data:image'):
                    img.decompose()
                    continue
                if not imgurl.startswith('http'): imgurl = self.urljoin(url, imgurl)
                yield (imgurl, img)
            else:
                img.decompose()
                continue

    def get_image_url(self, img):
        url = img['src'] if 'src' in img.attrs else None
        return url

    def edit_image(self, data, imgurl):
        try: return rescale_image(data, png2jpg = True, reduceto = self.setting["img_size"])
        except Exception as e:
            self.log.warn('Process image failed (%s):%s' % (imgurl, str(e)))
            return data

    def frag_to_html(self, title, content):
        frame = [
          '<!DOCTYPE html SYSTEM "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">',
          '<html xmlns="http://www.w3.org/1999/xhtml">',
          '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8">',
          '<title>%s</title></head><body><div>%s</div></body></html>'
        ]
        frame = ''.join(frame)
        return frame % (title, content)


# Feed 的父类，定义了解析 Feed 的通用方法
class BaseFeed(object):

    def parse_feed(self):
        # yield (section, title, url, html, brief)
        time = datetime.datetime.now()
        for feed in self.feeds:
            section, url = feed[0], feed[1]
            if self.check_feed_skip(time):
                self.log.warn( "Skip feed: %s" % (section) )
                continue
            content = self.featch_content(url)
            if content:
                content = feedparser.parse(content)
                for e in content['entries'][:self.setting["max_article"]]:
                    if self.check_article_skip(time, e): break
                    for section, title, url, html, brief in self.create_feed_content(e, section):
                        yield (section, title, url, html, brief)
            else: self.log.warn('Fetch feed failed: %s' % section)

    def check_feed_skip(self, time):
        if self.setting["deliver_days"] < 1: return False
        days = (time - datetime.datetime(2016,1,1)).days
        return days % self.setting["deliver_days"] != 0

    def check_article_skip(self, time, e):
        if self.setting["oldest_article"] < 1: return False
        updated = None
        if hasattr(e, 'published_parsed') and e.published_parsed: updated = e.published_parsed
        elif hasattr(e, 'updated_parsed') and e.updated_parsed: updated = e.updated_parsed
        elif hasattr(e, 'created_parsed'): updated = e.created_parsed
        if updated is None: return False
        updated = datetime.datetime(*(updated[0:6]))
        return (time - updated).total_seconds() > self.setting["oldest_article"] * 86400


# 网络爬虫的父类，定义了抓取网页内容的通用方法
class BaseSpider(object):

    def spider_main(self, detect = None, capture = set()):
        detect  = detect  # 待检测网址
        capture = capture # 待抓取网页
        done    = set()   # 已抓取网页
        cache   = {}      # 本次抓取结果的缓存
        result  = []      # 抓取结果
        task = capture if detect is None else (set([detect]) | capture)
        task = task - done
        while len(task) > 0:
            for url in task:
                cache[url] = None
                content = self.featch_content(url)
                if content: cache[url] = content
                else: self.log.warn('Fetch URL failed, skip(%s)' % url)
            for url in capture:
                if not url in done: result.append( (cache.get(url, None), url) )
            detect, capture = self.spider_refresh_capture(detect, cache.get(detect, None))
            done.add(url)
            cache = {}
            task = capture if detect is None else (set([detect]) | capture)
            task = task - done
        return result

    def spider_refresh_capture(self, url, html):
        # return detect_url, set([capture_url])
        if html is None: return None, set()
        return None, set()

    def spider_generate_html(self, result):
        content = u''
        for html, url in result:
            try:
                if not html: continue
                soup = self.process_article(html)
                content += unicode(soup.html.body)
            except Exception as e:
                self.log.warn('Creat html fail(%s):%s' % (url, str(e)))
                content += '<p>*** This Page Get Fail ***</p><a href="%s">Link</a>' % url
        return content


# 全文 RSS 书籍的类
class BaseFeedBook1(BaseFeed, BaseBook):

    title         = ''
    description   = ''
    mastheadfile  = DEFAULT_MASTHEAD
    coverfile     = DEFAULT_COVER
    deliver_times = []
    deliver_days  = []
    setting       = {}
    feeds         = []

    def get_items(self):
        # yield (section, title, url, soup, brief)
        for section, title, url, html, brief in self.parse_feed():
            yield (section, title, url, self.process_article(html), brief)

    def create_feed_content(self, e, section):
        # yield (section, title, url, html, brief)
        if hasattr(e, 'link'): url = e.link
        else: return
        summary = e.summary if hasattr(e, 'summary') else ""
        content = e.content[0]['value'] if (hasattr(e, 'content') and e.content[0]['value']) else ""
        html = content if len(content) > len(summary) else summary
        html = '<h1>%s</h1><div>%s</div>' % (e.title, html)
        html = self.frag_to_html(e.title, html)
        yield (section, e.title, url, html, None)


# 非全文 RSS 书籍的类
class BaseFeedBook2(BaseSpider, BaseFeed, BaseBook):

    title         = ''
    description   = ''
    mastheadfile  = DEFAULT_MASTHEAD
    coverfile     = DEFAULT_COVER
    deliver_times = []
    deliver_days  = []
    setting       = {}
    feeds         = []

    def get_items(self):
        # yield (section, title, url, soup, brief)
        for section, title, url, html, brief in self.parse_feed():
            yield (section, title, url, BeautifulSoup(html, "lxml"), brief)

    def create_feed_content(self, e, section):
        # yield (section, title, url, html, brief)
        if hasattr(e, 'link'): url = e.link
        else: return
        result = self.spider_main(capture = set([url]))
        html = self.spider_generate_html(result)
        html = '<h1>%s</h1><div>%s</div>' % (e.title, html)
        html = self.frag_to_html(e.title, html)
        yield (section, e.title, url, html, None)


# 抓取网页内容生成书籍内容的类
class BaseWebBook(BaseSpider, BaseBook):

    title         = ''
    description   = ''
    mastheadfile  = DEFAULT_MASTHEAD
    coverfile     = DEFAULT_COVER
    deliver_times = []
    deliver_days  = []
    setting       = {}
    feeds         = []

    def get_items(self):
        # yield (section, title, url, soup, brief)
        for feed in self.feeds:
            section, url = feed[0], feed[1]
            title = section
            result = self.spider_main(capture = set([url]))
            html = self.spider_generate_html(result)
            html = self.frag_to_html(title, html)
            yield(section, title, url, BeautifulSoup(html, "lxml"), None)


# 杂志的类，一本杂志由多本书籍组成
class BaseMagazine(Base):

    title         = ''
    description   = ''
    mastheadfile  = DEFAULT_MASTHEAD
    coverfile     = DEFAULT_COVER
    deliver_times = []
    deliver_days  = []
    setting       = {}
    book_list     = []

    def get_items(self, opts = None, user = None):
        # yield (section, url, title, content, brief, thumbnail)
        i = 0
        for book_class in self.book_list:
            try:
                book = book_class(imgindex = i, setting = self.setting)
                for data in book.Items(opts,user): yield data
                i = book.imgindex
            except Exception as e:
                default_log.warn("Failure in pushing book '%s' : %s" % (book_class, str(e)))

    def Items(self, opts = None, user = None):
        # yield (section, url, title, content, brief, thumbnail)
        for section, url, title, content, brief, thumbnail in self.get_items(opts, user):
            yield (section, url, title, content, brief, thumbnail)


def debug_mail(content, name='page.html'):
    from google.appengine.api import mail
    mail.send_mail(SRC_EMAIL, SRC_EMAIL, "KindleEar Debug", "KindlerEar",
        attachments=[(name, content),])
