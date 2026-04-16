from setuptools import setup, find_packages
 
setup(
    name="AQF", #Name
    version="1.0", #Version
    packages = find_packages(),  # Automatically find the packages that are recognized in the '__init__.py'.
    description = "Data Quality Framework for Spark-based pipelines", #Description
    author = "qjener",
    author_email = "dasxxq@gmail.com"

)