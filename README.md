# Automated XML File creation using Python

*Adapted from a [medium.com](https://medium.com/analytics-vidhya/automated-xml-file-creation-using-python-877c24feba66) article.*

This is an end-to-end solution for creation of an XML file, with minimal knowledge on Python, 
using a mySQL database (the connection can be modified to fetch from any database), and a configuration 
file (i.e. excel file). The configuration file will provide the following details:
- Structure of the XML
- Bound/Unbound nodes/tags
- SQL to be used for data extraction

**The configuration file:** 
This is an Excel file that contains 2 tabs. The contents of the file can also be placed in a table and data fetched 
from that, but for the purposes of this project, I kept the contents in an Excel spreadsheet.

Tab1(QueryDetails) contains the details for queries that need to be executed in order to provide the data for the tags 
in the XML file. 