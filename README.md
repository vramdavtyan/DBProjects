# Football analytics with MongoDB, MySQL, and Python.
##### Change the Branch for each phase

---

## Setup Instructions

1. **Clone the Repository**  
   Clone the repository to your local machine.

2. **Create a Data Directory**  
   Inside the `DBProjects` folder, create a new directory called `data`.

3. **Add the CSV Files**  
   Download the CSV files from Kaggle and place them inside the `data` directory.
   https://www.kaggle.com/datasets/moradi/football-statistics?select=shots.csv

4. **Create a Virtual Environment (Windows)**  
   Set up a Python virtual environment to manage project dependencies. You can do this by running the following commands in your terminal:

   ```bash
   python -m venv venv
   ```

   Once the environment is created, activate it with:

   ```bash
   .\venv\Scripts\activate
   ```

5. **Install Required Packages**  
   Install the required Python packages using the `requirements.txt` file:

   ```bash
   pip install -r requirements.txt
   ```

---

## How to Run the Project

### To get started, simply execute the following Jupyter notebooks:

- **MongoQueries.ipynb**
- **MySqlQueries.ipynb**

The Python scripts contain functions that handle database management, DataFrame operations, and other utility functions used throughout the project.
