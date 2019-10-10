## Pastebin crawler
This mini project can crawls pastebin.com every 2 minutes and saves all recent 
pastes into mongodb containr.

### Prerquisites
- Docker

### Run instructions:
1. ```bash
git clone 
```

```bash
docker-compose up
```

```bash
docker-compose stop
```

Architecture from wiki: 
![alt text](https://upload.wikimedia.org/wikipedia/commons/d/df/WebCrawlerArchitecture.svg "architecture")


dict of root sites to look through
    dict of leafs per site
    
queue of links and according xpath or type of work or type of link

tuple url xpath

if url is pastebin/archive
    push xpath results to url queue
   
if url is pastebin/content
    run xpath from list
    push results to mongo db
    
run in threads
pop url from queue
choose weather to use paste or archive func
fetch the page
parse with lxml and ready xpath
push results to according data storage
     