## [IH] Data Project [M1 DATA MADPT 2020]

![Image](https://res.cloudinary.com/dute7e5ne/image/upload/v1593836752/portada_git_yjbtc6.png)

---

### :pushpin: **About**
This program will produce responses for the challenges ask for the final exercise in Module 1 of Data Analytics in IronHack's Bootcamp [PT2020]

This is an exercise on constructing a DATA PIPELINE, showcasing the programming skills and tools acquired in the first module of the program:
> ''A data pipeline views all data as streaming data and it allows for flexible schemas. Regardless of whether it comes from static sources or from real-time sources, the data pipeline divides each data stream into smaller chunks that it processes in parallel, conferring extra computing power.'''

#### Data Sources

* Tables (.db). In the following link you can find the .db file with the main dataset:
* API. The projects used the API from [Open SKills Project](http://dataatwork.org/data/)
* Web Scraping. The project retrieves information about ISO 3166 alpha2 code in [Wikipedia](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2) and from [World Health Organization](https://www.euro.who.int/en/countries) to get the desired countries inside Europe.


![Image](https://res.cloudinary.com/dute7e5ne/image/upload/v1593836752/estructura_git_adymbf.png)

### :computer: **Technology stack**
As a prerequisite, the programming lenguage of this repository is Python 3.7.3, therefore must have Python 3 installed.
- Argparse
- Datetime
- Os
- Functools
- Concurrent.futures
- Re

Furthermore, it is need to be installed in the proper environment the following libraries:
- SQL Alchemy (v.1.3.17)
- Pandas (v.0.24.2)
- Numpy (v.1.18.1)
- Requests (v.2.23.0)
- Beautiful Soup (v.4.9.1)
- Scikit-learn (v.0.23.1)
- Matplotlib (v.3.1.2)


#### Status
Version 1.0 [04.07.2020] > First version done for class presentation


### :wrench: **How to Use**
1. Download the repo (make sure you have fulfilled the prerequisites).
2. Run the function \ main_script.py \ and set a valid argument. There is only the option for a country argument. If the country is not in the generated information retrieved from the web sources, the program will exit at the beginning.

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
    │   ├── module_awrangling.py
    ├── package_analysis
    │   ├── module_analysis.py
    ├── package_reporting
    │   ├── module_reporting.py
    └── data
        ├── raw
             ├── ddbb
        ├── processed
             ├── (here you will find each ddbb table cleaned)
        └── results
             ├── df_percentage_by_job_and_gender.csv
             ├── df_top_skills.csv
             ├── viz_distribution_top_skills.png
             ├── viz_distribution_basic_income.png

```

### :love_letter: **Contact info**
linkedin.com/in/lauramielgo for inqueries.

---

