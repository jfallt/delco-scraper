from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_selenium_res():
    """
    Randomize user agent for selenium and initiate a driver
    """
    # Create random user agent
    ua = UserAgent()
    user_agent = ua.random
    print(user_agent)

    # Select options for selenium
    options = Options()
    options.headless = True
    options.add_argument('--window-size=1920,1200')
    options.add_argument(
        'user-agent={user_agent}'.format(user_agent=user_agent))

    # Add driver path and create driver
    DRIVER_PATH = '/usr/local/share/chromedriver'
    return webdriver.Chrome(options=options, executable_path=DRIVER_PATH)


if __name__ == "__main__":
    get_selenium_res()
