Let's create a project together in the folder `crawler`. We are building an experimental/educational web crawler with 4 main goals:
1. Run on a single machine in one-shot (batch process, not continuous), aiming to crawl 50 million pages in 24 hours, starting from a seed file of domains provided as newline-separated strings, storing text content of the crawled web pages for later processing.
2. Respect politeness, including robots.txt, having minimum time of 70 seconds between re-crawls of the same domain, an explanation in the user agent of the crawler about the project (including contact email, which should be required to run the crawler program), and an optional way to provide a list of domains to be excluded from crawling.
3. Fault tolerance, including ability to stop and resume crawling, meaning we should keep track of pages that have already been crawled, the frontier of URLs to be crawled, and other state such as excluded domains in a persistent way.
4. A way to inspect the progress of the crawler, including the URLs crawled and the text content of each (this can be done with tools outside of the main program; instructions for doing so can suffice, another program is not needed).

Non goals:
- Handle non-text data.
- Handle pages that require login or other challenging dynamic content.

The key parts of the program should be written in Python, but feel free to use non-Python modules or dependencies where needed. However, as the project is educational, we should try to maintain consistency and clean design.

Work only within the `crawler` folder. Do not edit any files outside. Create a plan including the architecture, writing to `PLAN.MD`, and ask for my approval before beginning to code. In the plan, feel free to add any features which you feel are important, but make sure to honor the theme of education/simplicity. The crawler will be deployed to a cloud machine running linux (ubuntu); feel free to ask additional questions and provide suggestions about the running of the crawler while making the plan. 

## Meta

- Cursor agent mode
- Gemini 2.5 Pro MAX
- 304 requests