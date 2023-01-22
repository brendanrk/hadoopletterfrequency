import requests
from bs4 import BeautifulSoup
import os
from tqdm import tqdm

def download_file(url, folder):
    local_filename = url.split('/')[-1][:-6]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print("saving", folder + "/" + local_filename)
        with open(folder + "/" + local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=None):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                if chunk:
                    f.write(chunk)
    return local_filename


languages = ["es", "de", "it", "fr"]

for language in languages:
    os.system("mkdir " + language)
    url = 'https://www.gutenberg.org/browse/languages/' + language

    # Connect to the URL
    response = requests.get(url)

    # Parse HTML and save to BeautifulSoup object¶
    soup = BeautifulSoup(response.text, "html.parser")

    end = False
    count = 1
    for one_a_tag in tqdm(soup.findAll('a')):  # 'a' tags are for links
        if end:
            break
        if 'href' in one_a_tag.attrs:
            if one_a_tag.attrs['href'].startswith("/ebooks"):
                print(one_a_tag.attrs['href'])
                link_txt = 'https://www.gutenberg.org' + one_a_tag.attrs['href'] + ".txt.utf-8"
                print(link_txt)
                response = requests.get(link_txt)
                if response.status_code != 200:
                    print("NOT A LINK")
                else:
                    link = 'https://www.gutenberg.org' + one_a_tag.attrs['href']
                    r = requests.get(link)
                    trs = BeautifulSoup(r.text, "html.parser").findAll('tr')
                    count_langs = 0
                    for tr in trs: # make sure we don't have a book with multiple languuages
                        if 'itemprop' in tr.attrs:
                            if tr.attrs['itemprop'] == "inLanguage":
                                count_langs += 1
                    print("count_langs:", count_langs)
                    if count_langs < 2:
                        count += 1
                        print("Total (so far):", count)
                        if count > 50:
                            end = True
                        print(download_file(link_txt, language))
    print("Total:", count)
