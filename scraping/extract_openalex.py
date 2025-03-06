import os
import pandas as pd 
import pickle as pkl 
import requests
import time

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Set directory where PDFs are saved
# !!! SPECIFY DIRECTORY PATH FROM ROOT !!!
output_dir = "/home/theo/D4G/13_democratiser_sobriete/scraping/ingested_articles"
dummy_dir = "/home/theo/D4G/13_democratiser_sobriete/scraping/dummy_dir"

# Request OpenAlex API
def search_openalex(query: str, cursor="*", per_page:int = 50, from_doi:bool = False, dois:list = None) -> dict:
    
    if dois is None :
        dois = []
        
    if from_doi :
        pipe_separated_dois = "|".join(dois)
        params = {
            "filter": f"open_access.is_oa:true,doi:{pipe_separated_dois}",
            "cursor": cursor,
            "per-page": per_page,
        }
    else :
        params = {
            "filter": "open_access.is_oa:true",
            "search" : f'{query}',
            "cursor" : cursor,
            "per-page": per_page,
            }

    url = "https://api.openalex.org/works"
    response = requests.get(url, params=params)
    response.raise_for_status()
    query_data = response.json()
    return query_data

# Retrieve PDF urls and OpenAlex IDs
def get_urls_to_fetch(query_data: dict) :
    urls_to_fetch = []
    filenames = []
    for i in range(len(query_data["results"])) :
        file_title = query_data["results"][i]["id"]
        filenames.append(file_title.split('/')[-1])
        try : 
            urls_to_fetch.append(query_data["results"][i]["best_oa_location"]["pdf_url"])
        except TypeError :
            urls_to_fetch.append(query_data["results"][i]["open_access"]["oa_url"])
    return urls_to_fetch, filenames

# Start Selenium webdriver for scraping
def start_webdriver() -> webdriver.Chrome:
    chrome_options = webdriver.ChromeOptions()
    #chrome_options.add_argument("--headless=new")
    prefs = {
        "download.default_directory": dummy_dir, #change default directory for downloads
        "savefile.default_directory": dummy_dir, 
        "download.prompt_for_download": False, #to auto-download the file
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True #it will not show PDF directly in chrome
    }
    chrome_options.add_experimental_option('prefs', prefs)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(options = chrome_options, service = service)
    driver.set_page_load_timeout(30) #to ensure script doesn't hang in the event of a bad url
    return driver

# Function to empty a directory (mostly to make sure dummy_dir stays empty)
def clear_directory(directory: str) -> None:
    files_in_dir = os.listdir(directory)
    for file in files_in_dir :
        os.remove(os.path.join(directory, file))

# Tell driver to navigate to the given url, download the pdf and then rename the last downloaded file
def download_pdf(url: str, driver: webdriver.Chrome, filename: str, maxwait=30) -> str:
    def get_last_downloaded_file_path(dummy_dir) -> str:
        """ Return the last modified -in this case last downloaded- file path.
            This function is going to loop as long as the directory is empty.
        """
        while not os.listdir(dummy_dir):
            time.sleep(1)
        return max([os.path.join(dummy_dir, f) for f in os.listdir(dummy_dir)], key=os.path.getctime)

    try : 
        driver.get(url)
        time.sleep(1)
        # if download doesn't start for some reason, exit the function
        if not os.listdir(dummy_dir) :
            return "Failed"
        waittime = 0
        # fix filename by waiting for file in dummy_dir to be correctly downloaded, then rename it and put it in output_dir
        while not get_last_downloaded_file_path(dummy_dir).endswith(".pdf") :
            time.sleep(1)
            waittime += 1
            if waittime > maxwait :
                clear_directory(dummy_dir)
                return "Failed"
        os.rename(get_last_downloaded_file_path(dummy_dir), f"{os.path.join(output_dir, 'pdf_files', filename)}.pdf")
        return "Success"
    except WebDriverException :
        return "Failed"

# Function that puts everything together to extract all pdf files and metadata from a single query
def scrape_all_urls(driver: webdriver.Chrome, 
                    query: str, 
                    per_page:int = 50, 
                    start_from_scratch:bool = False, 
                    stop_criterion=None, 
                    maxwait:int = 60, 
                    from_dois:bool = False) -> None:
    """
    - driver : chromedriver instance initialized from start_webdriver function
    - query : a query passed to the OpenAlex API if not downloading from a list of DOIs
    - per_page : number of results per page listed for each call to the OpenAlex API, set it to 100 max if using from_dois
    - start_from_scratch : for a given query, start the ingestion from the start (1st page of returned by the API).
    If True, starts from last saved checkpoint (checkpoints should be handled automatically)
    - stop_criterion : number of articles to ingest before exiting the while loop (leave it as None if you want to download all obtained results)
    - maxwait : maximum time allowed for the download of a single PDF before moving on to the next one
    - from dois : if True, get list of DOIs from file list_dois.csv and ingest related articles. 
    Checkpoint management probably doesn't work in that case, better to just trim list_dois.csv before calling this function  
    """


    # initialize and clean output directories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'pkl_files'), exist_ok=True)
    os.makedirs(os.path.join(output_dir, 'pdf_files'), exist_ok=True)
    os.makedirs(dummy_dir, exist_ok=True)
    clear_directory(dummy_dir)

    all_files = os.listdir(output_dir)
    parsed_files = [1 for item in all_files if ".pkl" in item]
    
    start_time = time.time()
    failed_downloads = 0

    # start extraction from scratch or from a checkpoint
    if start_from_scratch :
        cursor = "*"
    else :
        download_checkpoint = pkl.load(open('download_checkpoint.pkl', 'rb'))
        cursor = download_checkpoint["cursor"]
    
    if not stop_criterion :
        stop_criterion = 1e10 # effectively infinite number, call API until all query results have been handled
    
    # get dois from file, can probably be improved
    if from_dois :
        assert os.path.isfile("./list_dois.csv"), "'list_dois.csv' not found in current directory"
        dois = pd.read_csv('list_dois.csv')["dois"].tolist()
        query = ""
        doi_idx = 0
    else :
        dois = []
    
    first_pass = True
    # extracting pdfs until stop_criterion is reached or until end of query is reached (last cursor)
    while len(parsed_files) < stop_criterion :
        # limit of 100 dois that can be passed in one API call, creating chunks for each iteration
        if from_dois :
            assert per_page <= 100, "Can only call the API with up to 100 DOIs in a single call, set per_page to 100 or lower"
            sub_dois = dois[doi_idx:doi_idx+per_page]
        else :
            sub_dois = []
        query_data = search_openalex(query, cursor = cursor, per_page = per_page, from_dois = from_dois, dois = sub_dois)
        urls_to_fetch, filenames = get_urls_to_fetch(query_data)
        
        # on first iteration, see how many files will need to be parsed
        if first_pass :
            if from_dois :
                total_filecount = len(dois)
            else :
                total_filecount = query_data["meta"]["count"]
            print(f"Preparing to download {total_filecount} articles...")
            first_pass = False
        
        # Extraction starts here :
        # first save all article metadata with same filename as pdf file
        for i in range(len(query_data["results"])) :
            data = query_data["results"][i]
            filename = filenames[i]
            with open(f"{os.path.join(output_dir, 'pkl_files', filename)}.pkl", "wb") as outfile :
                pkl.dump(data, outfile)
            outfile.close()
        
        # then save all pdf files with new filename
        for i in range(len(urls_to_fetch)) :
            url = urls_to_fetch[i]
            filename = filenames[i]
            print(f"\rRetrieving article {doi_idx + i}/{total_filecount}", end = "")
            # catch empty urls, move on to the next one if empty
            if not url :
                failed_downloads += 1
                continue
            download_status = download_pdf(url, driver, filename, maxwait)
            if download_status == "Failed" :
                failed_downloads += 1
        
        # navigate to next page of query results from OpenAlex
        cursor = query_data["meta"]["next_cursor"]
        if not cursor :
            break
        all_files = os.listdir(output_dir)
        parsed_files = [1 for item in all_files if item.endswith(".pkl")]
        doi_idx += per_page

        # new checkpoint to restart extraction if start_from_scratch == False
        with open("download_checkpoint.pkl", "wb") as outfile :
            pkl.dump({"cursor" : cursor}, outfile)
        outfile.close()
    
    print(f"""\nTotal elapsed time for {total_filecount} files : {time.time() - start_time}
              \nFailed downloads : {failed_downloads}
              \nSuccessful downloads : {sum([1 for file in os.listdir(os.path.join(output_dir, 'pdf_files'))])}""")
    
def main(query:str = "", 
         from_dois:bool = False,
         per_page:int = 100,
         stop_criterion = None, 
         maxwait:int = 60, 
         start_from_scratch:bool = False) -> None:
    
    print(f"""Downloading results to path : {output_dir}.
          Make sure to give the full path (from root), otherwise automatic file renaming won't be functional.""")

    driver = start_webdriver()
    scrape_all_urls(driver=driver,
                    query=query, 
                    from_dois=from_dois, 
                    per_page=per_page, 
                    start_from_scratch=start_from_scratch, 
                    stop_criterion=stop_criterion, 
                    maxwait=maxwait)
