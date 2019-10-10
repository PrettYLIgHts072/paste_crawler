## Pastebin crawler
This mini project can crawl [pastebin.com](www.pastebin.com) at intervals(default 2 minutes) and save
 all recent pastes into MongoDB.
 Both crawler and mongo are defined in docker-compose and ready to run.
 It uses the Pythons asyncio library to run asynchronous HTTP requests in order to save
 time on IO.
  
 Be aware I haven't set up authentication since anyway it is submitted to GitHub. 

### Prerequisites
- Docker

### Run instructions:
1. `git clone https://github.com/NescobarAlopLop/paste_crawler`
2. `cd paste_crawler`
3. `docker-compose up` or  `docker-compose up -d` to run in daemon mode.

### More Info:
In project root you can find [crawler_config.yml](https://github.com/NescobarAlopLop/paste_crawler/blob/master/crawler_config.yml)
there all the basic settings are defined. For example, features to extract, database details, and
XPath rules to extract data from collected HTML pages, seed URL.

I have found that using random HTML headers for HTML GET requests prolongs the time before
Pastebin blocks the crawler IP, they do allow some free crawling but it is limited.
So feel free to add more headers, they are chosen at random before each request.



Architecture diagram from wiki that roughly describes the crawler: 
![alt text](https://upload.wikimedia.org/wikipedia/commons/d/df/WebCrawlerArchitecture.svg "architecture")

There are some changes in the directions of the arrows..

### Main features:
Crawler gathers each one of the new "pastes" from Pastebin, and parses it into
 the following structure:
- Author - String
- Title - String
- Content - String
- Date - Date


Each one of the paste model's parameters is normalized:
* Author - In cases it's Guest, Unknown, Anonymous is converted to "" (empty string)
* Title - Same as with Author.
* Date - UTC Date
* Content - Is stripped of trailing spaces.

As already mentioned results are saved to MongoDB container.

Libraries used for the project:
- requests (http client)
- lxml (html parsing)
- pymongo (storage)
- arrow (dates)
