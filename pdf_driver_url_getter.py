from selenium import webdriver
from selenium.webdriver.chrome.service import Service
# For improved reliability, you should consider using WebDriverWait in combination with element_to_be_clickable.
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

import time
import pandas as pd

URL = "https://www.google.com/search?q=filetype%3Apdf+whitbread+annual+report&oq=filetype%3Apdf+whitbread+annual+report&aqs=edge..69i57j69i58j69i64.13319j0j1&sourceid=chrome&ie=UTF-8"
URL = "https://www.google.com/search?q=filetype%3Apdf+whitbread+annual+report"


def accept_cookies(driver):
    '''Accepts the cookies pop-up on the main page'''
    # time.sleep(3)
    actions = ActionChains(driver)
    for _ in range(4):
        actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.ENTER)
    actions.perform()
    # time.sleep(3)

chrome_driver_path = "C:/Users/Sulayman/Desktop/code_directory/ChromeDrivers/chromedriver_win32 (1)/chromedriver"
service = Service(f'{chrome_driver_path}.exe')
# TODO pick up options to minimise chances of getting blocked?
chrome_search_query = "https://www.google.com/search?q="
# driver = webdriver.Chrome(executable_path=chrome_driver_path)
driver = webdriver.Chrome(service=service)#, options=options)


driver.get(URL)
accept_cookies(driver)

def open_return_url():
    # would be improved by _not_ opening the pdf: just retrieving the ASYNC_URL from google search

    # TODO formulate search query using company name
    _URL = 'url'
    driver.get(_URL)

    result = driver.find_element(By.TAG_NAME, 'h3')
    driver.execute_script("arguments[0].click();", result) # why does this work and 'click' just doesn't..

    return driver.current_url

#urls = driver.find_element("xpath", '//div[@class="yuRUbf"]//a[@href]')
#urls = driver.find_element(By.CLASS_NAME, "yuRUbf")
