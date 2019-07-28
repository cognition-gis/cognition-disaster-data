from lxml import etree
import requests
from io import StringIO

postprocess = lambda x: x.replace("\\r\n", " ").replace("\\n", " ") if x != "\\n" else ''

def parse_fgdc(url):

    page = requests.get(url)
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(str(page.content)), parser)

    md = {}
    # Build FGDC metadata dict from URL
    for item in tree.xpath('//dl/dt/em'):
        header = item.text[:-2].replace(' ', '')

        # This is super hacky but it works
        val = [
            postprocess(x) for x in item.getparent().xpath('text()') if len(postprocess(x)) > 0
        ]

        if header in md:
            md[header] += val
        else:
            md.update({header: val})

    # Do some post processing to the dict
    for item in md:
        if len(md[item]) == 0:
            md[item] = None
        elif len(md[item]) == 1:
            md[item] = md[item][0]

    return md

def format_datetime(date, time=None):
    date = f"{date[0:4]}-{date[4:6]}-{date[6:8]}"
    if time:
        return f"{date}T{time.split(' ')[0]}.00Z"
    else:
        return date

def temporal_window(md):
    return [
        format_datetime(md.get('BeginningDate'), md.get('BeginningTime')),
        format_datetime(md.get('EndingDate'), md.get('EndingTime'))
    ]

