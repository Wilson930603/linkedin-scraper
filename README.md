### Linkedin Scraper
This is a simple Linkedin scraper that uses Scrapy to scrape the Linkedin profiles of people who work at a company.
The data that it fetches are as follows:
- Profile:
  - First Name
  - Last Name
  - Contact Info (email, twitter)
  - Location
  - Total Followers
  - About
  - Current Experience & Past Experience
    - Role
    - Company
    - Start Date
    - End Date
    - Present?
    - Description
  - Honors and Awards
    - Name
    - Date
    - Description
  - Recommendations
    - Name
    - Role
    - Profile URL
    - Header
    - Description
  - Licences
    - Name
    - Issuing Organization
    - Issue Date
    - Expiration Date
    - Credential ID
    - Credential URL
  - Posts
    - Text
    - Article Name
    - Link
    - Time of Post
    - N° of Likes
    - N° of Comments
    - Original Post
  - Comments
    - Post Contact Name
    - Post Description
    - Contact comment
    - Comment time
  - Reactions
    - Name of Contact
    - Contact Role
    - Post Details
  - Company in Header
    - Name
    - About
      - Overview
      - Website
      - Industries
      - Company Size
      - Headquarters
      - Founded
      - Specialties
    - Posts
      - Text
      - Article Name
      - Link
      - Time of Post
      - N° of Likes
      - N° of Comments
      - Original Post
    - Jobs
      - Title
      - Description
      - URL


### How to use
1. Clone the repository
2. You need to have Python 3.6+ installed
3. Install requirements
```bash
pip install -r requirements.txt
```
4. Add cookies to the file cookies.txt from your account
   * Open Google Chrome Browser.
   * Go to https://linkedin.com and login.
   * Open https://chrome.google.com/webstore/detail/editthiscookie/fngmhnnpilhplaeedifhccceomclgfbg?hl=en in a new tab and click on Add to Chrome.
   * Now, get back to the tab where you are logged in to LinkedIn.
   * Click on the EditThisCookie icon and click on Export and Export as JSON.
   * Open the cookies.txt file and replace the content with the content you exported.
5. Add a list of users to scrap to list.txt
6. Run the scraper
```bash
scrapy crawl linkedin_spider
```
7. The data will be saved in folder results for each user in a separate json file
