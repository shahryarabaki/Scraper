import sys, re, urllib, urlparse, time, os, sys
import multiprocessing as mp
from multiprocessing import Pool, TimeoutError, Queue
from lxml import html
from urllib2 import Request, urlopen
from loginform import _pick_form
import traceback
def is_login(url, body):
    userfield = passfield = emailfield = None
    _is_login = False
    doc = html.document_fromstring(body, base_url=url)
    try:
        form = _pick_form(doc.xpath('//form'))
    except Exception as ex:
        return False
    for x in form.inputs:
        if not isinstance(x, html.InputElement):
            continue

        type_ = x.type
        if type_ == 'password' and passfield is None:
            passfield = x.name
            _is_login = True
            break
        #elif type_ == 'email' and emailfield is None:
        #    emailfield = x.name
        #    _is_login = True
        #    break

    return _is_login

def queueURLs(body, origLink, dupcheck, q):
    for url in re.findall('''<a[^>]+href=["'](.[^"']+)["']''', body, re.I):
        link = url.split("#", 1)[0] if url.startswith("http") else '{uri.scheme}://{uri.netloc}'.format(uri=urlparse.urlparse(origLink)) + url.split("#", 1)[0]
        if link in dupcheck:
            continue
        dupcheck[link] = 1
        q.put(link)

def getHTML(link,url_list, dupcheck_crawl, dupcheck_logins, request_header, q, output_queue):
    try:
        request = Request(url=link, headers = request_header)
        body = urlopen(request).read()
                #html = requests.get(url=url, headers = request_header, timeout = 20).text
        #open(str(time.time()) + ".html", "w").write("" % link  + "n" + html)
        if is_login(link, body):
            fqdm = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse.urlparse(link))
            if fqdm not in dupcheck_logins:
                output_queue.put(link)
                dupcheck_logins[fqdm] = 1
            #print "has login ", link
        #else:
        #    print "does not have login ", link
        queueURLs(body, link, dupcheck_crawl,q)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as ex:
        pass

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Insufficient number of inputs"
        print "Usage: python scraper.py seed_file output_file"
        exit()
    try:
        print "Program started"
        total_urls = 0
        limit = 10000
        print "creating queues"
        m = mp.Manager()
        q = m.Queue()
        output_queue = m.Queue()
        url_list = []
        print "creating dicts"
        dupcheck_crawl = m.dict()
        dupcheck_logins = m.dict()
        request_header= {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:62.0) Gecko/20100101 Firefox/62.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q = 0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q = 0.7',
            'Keep-Alive': '300',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Accept-Language': '*',
            'Accept-Encoding': 'gzip, deflate'
        }
        print "creating pool"
        # start 4 worker processes
        pool = Pool(processes=100)
        print "Reading seed"
        with open(sys.argv[1], "r") as seed_file:
            for line in seed_file:
                url = line.strip().rstrip('\n')
                q.put(url)

        last_len = -1
        print "Creating processes"
        output_file = open(sys.argv[2], "w")
        checkpoints = 0
        while True:
            try:
                pool.apply_async(getHTML, (q.get(),url_list, dupcheck_crawl, dupcheck_logins, request_header, q, output_queue))
                list_len = output_queue.qsize()
                if list_len > limit:
                    print "Limit reached, break ", str(list_len)
                    output_queue.put(None)
                    break
                if list_len > 500:
                    checkpoints = checkpoints + 1
                    print "checkpoint ", checkpoints,  " reached"
                    output_queue.put("checkpoint")
                    for url in iter(output_queue.get, "checkpoint"):
                        print >>output_file, url

                if list_len % 2 == 0 and list_len != last_len:
                    print list_len
                    last_len = list_len
            except Exception as ex:
                print "Main ", ex

        print "Printing to file"
        for url in iter(output_queue.get, None):
            print >>output_file, url

        output_file.close()
        pool.terminate()
        pool.join()
    except Exception as ex:
        print ex
