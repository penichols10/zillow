import requests
from bs4 import Beautifulsoup
import pandas as pd

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"} 

url = 'https://www.zillow.com/rockville-md/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-77.36944914453125%2C%22east%22%3A-76.92450285546875%2C%22south%22%3A39.02324660629151%2C%22north%22%3A39.18469662654337%7D%2C%22usersSearchTerm%22%3A%22Rockville%2C%20MD%22%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A33714%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%2C%22ah%22%3A%7B%22value%22%3Atrue%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A11%7D'
req = requests.get(url)

print(req.content)