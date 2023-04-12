- [Team Members](#team-members)
- [Setup](#setup)
  - [1. Git clone the repository](#1-git-clone-the-repository)
  - [2. Setup python environment](#2-setup-python-environment)
    - [2.a. Create a venv environment](#2a-create-a-venv-environment)
    - [2.b. Install requirements.txt file using pip](#2b-install-requirementstxt-file-using-pip)
  - [3. Setup the relational database](#3-setup-the-relational-database)


<br/>

# Team Members


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

```git clone https://github.com/advaithsrao/694Team14Dbms2023.git```

<br/>

## 2. Setup python environment

> **NOTE:** A quick way to implement this would be with the *venv* package. You can also setup the environment with conda using the command ~conda create -n <env_name> python=3.8 | conda activate <env_name>~ . After this follow step 2.b 

### 2.a. Create a venv environment

```python3 -m venv <env_name>```

### 2.b. Install requirements.txt file using pip

```pip3 install -r requirements.txt --no-cache-dir```

<br/>

## 3. Setup the relational database

```sh scripts/setup/relationalDB.sh```

---