## [IH] Data Project [M1 DATA MADPT 2020]

![Image](https://res.cloudinary.com/dute7e5ne/image/upload/v1593836752/portada_git_yjbtc6.png)

---

### :pushpin: **About**
This program will produce responses for the challenges ask for the final exercise in Module 1 of Data Analytics in IronHack's Bootcamp [PT2020]

This is an exercise on constructing a DATA PIPELINE, showcasing the programming skills and tools acquired in the first module of the program:
> ''A data pipeline views all data as streaming data and it allows for flexible schemas. Regardless of whether it comes from static sources or from real-time sources, the data pipeline divides each data stream into smaller chunks that it processes in parallel, conferring extra computing power.'''

#### :stars: Data Sources

* Tables (.db). In the following link you can find the .db file with the main dataset:
* API. The projects used the API from [Open SKills Project](http://dataatwork.org/data/)
* Web Scraping. The project retrieves information about ISO 3166 alpha2 code in [Wikipedia](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) and from [World Health Organization](https://www.euro.who.int/en/countries) to get the desired countries inside Europe.

---
#### :warning: Project Requirements

##### **Challenge 1:** 
You need to create a ***Data Pipeline*** that retrieves the following table:

| Country | Job Title      | Gender  | Quantity | Percentage |
|---------|----------------|------------|----------|------------|
| Spain   | Data Scientist | Male       | 25       | 5%         |
| Spain   | Data Scientist | Female     | 25       | 5%         |
| ...     | ...            | ...        | ...      | ...        |
** Percentages are in proportion to each gender in each job category for each country


##### **Challenge 2:** 
The main purpuse of this challenge was to work with in favor/againsts polls, which where complex to clean from raw data.

My interpretation of the second challenge was to visually represent how different gender responded to the basic income polls and wether there was a significally difference. 


| Position  | Pro Arguments for Male | Pro Arguments for Female |
|-----------|------------------------|--------------------------|
| Responses |                        |                          |

| Position  | Against for Male       | Against  for Female      |
|-----------|------------------------|--------------------------|
| Responses |                        |                          |



##### **Challenge 3:**
The main purpose of this challenge was to work with education level table, with a discrete qualitative classification of levels.

I framed the challenge to continue working with gender differences within the data collected, therefore my final table shows top 5 jobs for each gender, using matplotlib to visualize quantities.

| Education Level | Top 5 Skills | 
|-----------------|--------------|
| high            |              |                          
| medium          |              |                          
| low             |              |                          
| no education    |              |                          

---
### :computer: **Technology stack**
As a prerequisite, the programming lenguage of this repository is Python 3.7.3, therefore must have Python 3 installed. The native packages in use are:
- [Argparse](https://docs.python.org/3.7/library/argparse.html)
- [Datetime](https://docs.python.org/2/library/datetime.html)
- [Os](https://docs.python.org/3/library/os.html)
- [Functools](https://docs.python.org/3/library/functools.html)
- [Concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
- [Re](https://docs.python.org/3/library/re.html)

Furthermore, it is need to be installed in the proper environment the following libraries:
- [SQL Alchemy (v.1.3.17)](https://docs.sqlalchemy.org/en/13/intro.html)
- [Pandas (v.0.24.2)](https://pandas.pydata.org/pandas-docs/stable/reference/index.html)
- [Numpy (v.1.18.1)](https://numpy.org/doc/stable/)
- [Requests (v.2.23.0)](https://requests.readthedocs.io/)
- [Beautiful Soup (v.4.9.1)](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [Scikit-learn (v.0.23.1)](https://towardsdatascience.com/preprocessing-with-sklearn-a-complete-and-comprehensive-guide-670cb98fcfb9)
- [Matplotlib (v.3.1.2)](https://matplotlib.org/contents.html)

### :star: **Project structure**

![Image](https://res.cloudinary.com/dute7e5ne/image/upload/v1594225796/unnamed_f3qugc.jpg)

#### :construction: Status
Version 1.0 [04.07.2020] > First version done for class presentation

Version 1.0.1 [08.07.2020] > Post presentation corrections 

---
### :wrench: **How to Use**
1. Download the repo (make sure you have fulfilled the prerequisites).
2. Run the function \ main_script.py \ and set a valid argument. There is only the option for a country argument. If the country is not in the generated information retrieved from the web sources, the program will exit at the beginning.
3. Possible inputs:
    - 3.1. View all countries contained in database:
```bash
$ python main_script.py -c All
```
You will get:
```bash
· Parsing argument: ['All']
	 ··· Fetching European countries from web scrapping
	 ··· Validating country argparse
		 >> getting all countries
```
   - 3.2. View a specific country in the ddbb:
```bash
$ python main_script.py -c United Kingdom
```
You will get:
```bash
 · Parsing argument: ['United', 'Kingdom']
	 ··· Fetching European countries from web scrapping
	 ··· Validating country argparse
 ·· country_argument found in ddbb
```  
   - 3.3. Wrong entries:
```bash
$ python main_script.py -c 
```
You will get:
```bash
 · Parsing argument: []
	 ··· Fetching European countries from web scrapping
	 ··· Validating country argparse
		 >> country_argument not found.
		 >> proceeding to exit
```
**Help from argeparse can always we call in doubt:
```bash
$ python main_script.py -help
```

---
### :love_letter: **Contact info**
linkedin.com/in/lauramielgo for inqueries.

### :hearts: **Thanks**
Big thanks to TAs and teachers for the help and support in the development of this project:

@github/potacho

@github/TheGurus

---

#### :file_folder: **Folder structure**
The folder structure follows the template given in class, generating as many files as necessary inside each package.

```
└── project
    ├── __trash__
    ├── .gitignore
    ├── .env
    ├── requeriments.txt
    ├── README.md
    ├── main_script.py
    ├── notebooks
    │   ├── acquisition.ipynb
    │   └── wrangling.ipynb
    ├── package_acquisition
    │   ├── module_acquisition.py
    │   └── module_cleaning.py
    ├── package_wrangling
    │   └── module_awrangling.py
    ├── package_analysis
    │   └── module_analysis.py
    ├── package_reporting
    │   └── module_reporting.py
    └── data
        ├── raw
             └── ddbb
        ├── processed
             └── (here you will find each ddbb table cleaned)
        └── results
             ├── df_percentage_by_job_and_gender.csv
             ├── df_top_skills.csv
             ├── viz_distribution_top_skills.png
             └── viz_distribution_basic_income.png

```

### :bookmark: **Code Beauty Pageant**

