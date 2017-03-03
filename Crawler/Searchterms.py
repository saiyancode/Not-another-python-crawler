from bs4 import BeautifulSoup

class search():
    def __init__(self, queue_url, body, search_terms, root_url):
        try:
            keywords_found, article = get_all_keyword_mentions(body,search_terms)
            link_keywords = find_all_hrefs(article,root_url,search_terms)
            final_list = compare(keywords_found, link_keywords)
            if len(final_list) > 0:
                write_to_file(queue_url, final_list)
        except Exception as e:
            print(e)
            pass

def get_all_keyword_mentions(html,search_terms):
    keywords_found = []
    soup = BeautifulSoup(html)
    article = soup.find('body')
    for vanity in search_terms:
        if vanity in str(article.get_text().lower()):
            if vanity not in keywords_found:
                keywords_found.append(vanity)
    return keywords_found, article


def find_all_hrefs(article,root_url,search_terms):
    link_keywords = []
    hrefs = article.find_all('a',href=True)
    for href in hrefs:
        if href['href'].startswith('/'):
            href['href'] = '{}{}'.format(root_url,href['href'])
        for vanity in search_terms:
            if vanity in str(href.get_text().lower()):
                if vanity not in link_keywords:
                    link_keywords.append(vanity)
    return link_keywords


def compare(keywords_found,link_keywords):
    empty_list = []
    for keyword in keywords_found:
        if keyword not in link_keywords:
            empty_list.append(keyword)
    return empty_list

def write_to_file(url,final_list):
    final_list = ','.join(final_list)
    with open('article.csv','a',encoding='utf-8') as out_file:
        out_file.write('{},{}\n'.format(url,final_list))
