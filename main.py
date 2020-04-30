import os
import requests
import pandas as pd
from tqdm import tqdm
import concurrent.futures

# insert here the folder you want the books to be downloaded:
folder = os.getcwd() + '/downloads/'

if not os.path.exists(folder):
    os.mkdir(folder)
    
if not os.path.exists("table.xlsx"):
    books = pd.read_excel('https://resource-cms.springernature.com/springer-cms/rest/v1/content/17858272/data/v4')
    # https://resource-cms.springernature.com/springer-cms/rest/v1/content/17863240/data/v2
    # save table:
    books.to_excel(folder + 'table.xlsx')
else:
    books = pd.read_excel('table.xlsx')  

# debug:
# books = books.head()

def GetList(books):
    aList = {}
    for url, title, author, pk_name in books[['DOI URL', 'Book Title', 'Author', 'English Package Name']].values:

        new_folder = folder + pk_name + '/'

        if not os.path.exists(new_folder):
            os.mkdir(new_folder)

        url2 = url.replace('http://doi.org/','https://link.springer.com/content/pdf/')
        url2 = url2 + '.pdf'
        final = url2.split('/')[-1]
        final = title.replace(',','-').replace('.','').replace('/',' ').replace(':',' ') + ' - ' + author.replace(',','-').replace('.','').replace('/',' ').replace(':',' ') + ' - ' + final
        output_file = new_folder+final
        aList[url2] = output_file
        
        url3 = url.replace('http://doi.org/','https://link.springer.com/download/epub/')
        url3 = url3 + '.epub'
        final = url3.split('/')[-1]
        final = title.replace(',','-').replace('.','').replace('/',' ').replace(':',' ') + ' - ' + author.replace(',','-').replace('.','').replace('/',' ').replace(':',' ') + ' - ' + final
        output_file = new_folder+final
        aList[url3] = output_file
    return aList

def GetBook(tup):
    ret = 1
    if not os.path.exists(tup[1]):
        myfile = requests.get(tup[0], allow_redirects=True)
        ret = myfile.status_code
        if ret == 200:
            try:
                open(tup[1], 'wb').write(myfile.content)
            except OSError: 
                ret = 2
    return ret


print('Download started.')

aList = GetList(books)
'''
for item in tqdm(aList.items()):
    GetBook(item)
'''
# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(GetBook, item): item[0] for item in aList.items()}
    for future in tqdm(concurrent.futures.as_completed(future_to_url),total=len(future_to_url)):
        url = future_to_url[future]
        try:
            data = future.result()
            msg = f"{url} Oops, status code = {data}"
        except Exception as exc:
            print('\n%r generated an exception: %s' % (url, exc))
        else:
            if data == 200:
                msg = f"\n{url} was downloaded!"
            elif data == 2:
                msg = f"\n{url} wrote error!"
        #print(msg)


print('Download finished.')