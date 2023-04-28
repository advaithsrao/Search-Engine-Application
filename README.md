- [Team Members](#team-members)
- [Setup](#setup)
  - [1. Git clone the repository](#1-git-clone-the-repository)
  - [2. Setup python environment](#2-setup-python-environment)
    - [2.a. Create a venv environment](#2a-create-a-venv-environment)
    - [2.b. Source onto environment](#2b-source-onto-environment)
    - [2.c. Install requirements.txt file using pip](#2c-install-requirementstxt-file-using-pip)
  - [3. Add search API data to data folder](#3-add-search-api-data-to-data-folder)
  - [3. Setup the SQL and NoSQL databases](#3-setup-the-sql-and-nosql-databases)
  - [4. Setup the Front End](#4-setup-the-front-end)
  - [5. Setup the periodic ttl based cacher](#5-setup-the-periodic-ttl-based-cacher)
- [Other information](#other-information)
  - [| 5601 | Kibana for ELK |](#-5601--kibana-for-elk-)


<br/>

# Team Members

**Team 14**

| Name | Net ID |
| ---- | ---- |
| Advaith Rao | asr209 |
| Ayush Oturkar | ao586 |
| Falgun Malhotra | fm466 |
| Vanshita Gupta | vg422 |

<br/>

# Setup

<br/>

## 1. Git clone the repository

```bash
git clone https://github.com/advaithsrao/694Team14Dbms2023.git
```

<br/>

## 2. Setup python environment

> **NOTE:** A quick way to implement this would be with the *venv* package. You can also setup the environment with conda using the command ```conda create -n <env_name> python=3.8 | conda activate <env_name>```. After this follow step 2.c 

### 2.a. Create a venv environment

```bash
python3 -m venv <env_name>
```

### 2.b. Source onto environment

```bash
source <env_name>/bin/activate
```

### 2.c. Install requirements.txt file using pip

```bash
pip3 install -r requirements.txt --no-cache-dir
```

<br/>

## 3. Add search API data to data folder

```bash
mkdir ./data
#copy data to this folder
```

## 3. Setup the SQL and NoSQL databases

```bash
sh scripts/setup/main.sh
```

<br/>

## 4. Setup the Front End

```bash
python3 UI/ui.py > /outputs/uiLogs.out & 
```

Once you run this, you should see the search app @ localhost:8000

<br/>

## 5. Setup the periodic ttl based cacher 

```bash
python3 scripts/staleCacheChecker.py > /outputs/cacherLogs.out & 
```

# Other information

| Port | Application |
| ---- | ---- |
| 8000 | Search App |
| 5544 | PostgreSQL |
| 9200 | Elastic Search (ELK) |
| 5601 | Kibana for ELK |
---
