import re
import sys
import time
import math
import urllib.request
import urllib.parse
import optparse
import hashlib
from html import escape
from traceback import format_exc
from queue import Queue, Empty as QueueEmpty
from bs4 import BeautifulSoup

class Link(object):
    def __init__(self, src, dst, link_type):
        self.src = src
        self.dst = dst
        self.link_type = link_type

    def __hash__(self):
        return hash((self.src, self.dst, self.link_type))

    def __eq__(self, other):
        return (self.src == other.src and
                self.dst == other.dst and
                self.link_type == other.link_type)

    def __str__(self):
        return self.src + " -> " + self.dst

class Crawler(object):
    def __init__(self, root, depth_limit, confine=None, exclude=[], locked=True, filter_seen=True):
        self.root = root
        self.host = urllib.parse.urlparse(root).hostname
        self.depth_limit = depth_limit
        self.locked = locked
        self.confine_prefix = confine
        self.exclude_prefixes = exclude
        self.urls_seen = set()
        self.urls_remembered = set()
        self.visited_links = set()
        self.links_remembered = set()
        self.num_links = 0
        self.num_followed = 0

        self.pre_visit_filters = [self._prefix_ok, self._exclude_ok, self._not_visited, self._same_host]
        
        self.out_url_filters = [self._prefix_ok, self._same_host] if filter_seen else []

    def _pre_visit_url_condense(self, url):
        base, frag = urllib.parse.urldefrag(url)
        return base

    def _prefix_ok(self, url):
        return (self.confine_prefix is None or url.startswith(self.confine_prefix))

    def _exclude_ok(self, url):
        return all(not url.startswith(p) for p in self.exclude_prefixes)

    def _not_visited(self, url):
        return url not in self.visited_links

    def _same_host(self, url):
        try:
            host = urllib.parse.urlparse(url).hostname
            return host == self.host
        except Exception as e:
            print(f"ERROR: Can't process url '{url}' ({e})", file=sys.stderr)
            return False

    def crawl(self):
        q = Queue()
        q.put((self.root, 0))
        while not q.empty():
            this_url, depth = q.get()
            if depth > self.depth_limit:
                continue

            do_not_follow = [f for f in self.pre_visit_filters if not f(this_url)]
            if depth == 0 and do_not_follow:
                print(f"Whoops! Starting URL {this_url} rejected by the following filters:", do_not_follow, file=sys.stderr)

            if not do_not_follow:
                try:
                    self.visited_links.add(this_url)
                    self.num_followed += 1
                    page = Fetcher(this_url)
                    page.fetch()
                    for link_url in [self._pre_visit_url_condense(l) for l in page.out_links()]:
                        if link_url not in self.urls_seen:
                            q.put((link_url, depth + 1))
                            self.urls_seen.add(link_url)
                        do_not_remember = [f for f in self.out_url_filters if not f(link_url)]
                        if not do_not_remember:
                            self.num_links += 1
                            self.urls_remembered.add(link_url)
                            link = Link(this_url, link_url, "href")
                            if link not in self.links_remembered:
                                self.links_remembered.add(link)
                except Exception as e:
                    print(f"ERROR: Can't process url '{this_url}' ({e})", file=sys.stderr)

class Fetcher(object):
    def __init__(self, url):
        self.url = url
        self.out_urls = []

    def __getitem__(self, x):
        return self.out_urls[x]

    def out_links(self):
        return self.out_urls

    def _open(self):
        url = self.url
        try:
            request = urllib.request.Request(url)
            handle = urllib.request.build_opener()
            return request, handle
        except IOError:
            return None, None

    def fetch(self):
        request, handle = self._open()
        if handle:
            try:
                data = handle.open(request)
                mime_type = data.info().get_content_type()
                url = data.geturl()
                if mime_type != "text/html":
                    raise OpaqueDataException(f"Not interested in files of type {mime_type}", mime_type, url)
                content = data.read().decode("utf-8", errors="replace")
                soup = BeautifulSoup(content, "html.parser")
                tags = soup('a')
                for tag in tags:
                    href = tag.get("href")
                    if href is not None:
                        url = urllib.parse.urljoin(self.url, escape(href))
                        if url not in self:
                            self.out_urls.append(url)
            except urllib.error.HTTPError as error:
                print(f"ERROR: {error}", file=sys.stderr)
            except urllib.error.URLError as error:
                print(f"ERROR: {error}", file=sys.stderr)


def getLinks(url):
    page = Fetcher(url)
    page.fetch()
    j = 1
    for i, url in enumerate(page):
        if "http" in url:
            print(f"{j}. {url}")
            j += 1

def parse_options():
    parser = optparse.OptionParser()
    parser.add_option("-l", "--links", action="store_true", default=False, dest="links", help="Get links for specified url only")
    parser.add_option("-d", "--depth", action="store", type="int", default=30, dest="depth_limit", help="Maximum depth to traverse")
    opts, args = parser.parse_args()
    if len(args) < 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    return opts, args

def main():
    opts, args = parse_options()
    url = args[0]
    if opts.links:
        getLinks(url)
        sys.exit(0)
    depth_limit = opts.depth_limit
    print(f"Crawling {url} (Max Depth: {depth_limit})", file=sys.stderr)
    crawler = Crawler(url, depth_limit)
    crawler.crawl()
    print(f"Found:    {crawler.num_links}", file=sys.stderr)
    print(f"Followed: {crawler.num_followed}", file=sys.stderr)

if __name__ == "__main__":
    main()
