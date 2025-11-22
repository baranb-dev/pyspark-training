

# 🚀 PySpark Challenge Repository

## Why do these exercises?

**Do these exercises before a technical interview or to prepare for a coding game!**

Practicing these challenges will help you:
- Refresh your PySpark skills
- Get comfortable with common data transformation tasks
- Build confidence for interviews or coding competitions

Good luck and have fun!


## 📚 Overview

This repository contains a collection of PySpark coding challenges, organized by difficulty. Each challenge is designed to help you practice and improve your PySpark and data engineering skills. You can implement your solution, compare it with the provided reference, and validate your approach.



## ⚡ Getting Started

1. **📥 Clone the repository:**
   ```sh
   git clone <https://github.com/baranb-dev/pyspark-training>
   cd pyspark-training
   ```

2. **🛠️ Set up your environment:**
  - (Recommended) Create a virtual environment:
     ```sh
     python3 -m venv .venv
     source .venv/bin/activate
     ```
  - Install dependencies:
     ```sh
     pip install -r requirement.txt
     ```
  - (Recommended) Copy pyspark conf ( change the python version if needed ):
     ```sh
     mkdir .venv/lib/python3.13/site-packages/pyspark/conf
     cp pyspark_conf/log4j2.properties .venv/lib/python3.13/site-packages/pyspark/conf
     ```

3. **🏃‍♂️ Run the challenges:**
  - You can run and test challenges from the `src/` directory.
    - Example:
     ```sh
     cd src
     spark-submit main.py
     ```


## 🏆 Challenge Structure


All challenges are located in `src/chall/` and are grouped by difficulty:


### 🟢 Easy
  - `chall_one_easy.py`
  - `chall_withcolumn_or_not.py`

### 🟡 Medium
  - `chall_one_medium.py`

### 🔴 Hard
  - `challenge_one_hard.py`
  - `challenge_two_hard.py`

Each challenge file contains a class that inherits from the abstract template in `chall_template.py`. Implement your solution in the `answer()` method and compare it with the `solution()` method.


## 📝 How to Use

1. Open a challenge file in your editor (e.g., `src/chall/easy/chall_one_easy.py`).
2. Implement your solution in the `answer()` method.
3. Run the main script to compare your answer with the reference solution.


## 🤝 Contribution

Feel free to add new challenges or improve existing ones! Please follow Python best practices and add clear docstrings to your code.


## 📄 License

MIT License. See [LICENSE](LICENSE) for details.

Thanks to david for his work in zillacode
https://github.com/davidzajac1/zillacode

