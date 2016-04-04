# etl
Project to implement ETL (Extract, Transform and Load) concept with JAVA 
The idea in a simple way, we want to create processes which allow multiple sub-processes with input (database query, SOAP webservice, RESTful webservice, CSV file or flat files)

#Sample
```<Process>
  <Database ....>
    <Query .../>
    <Process>
    <SOAPWebservice ...>
    </SOAPWebservice/>
    </Process>
  </Database>
  <RESTfulWebservice ...>
    <CSVFile .../>
  </RESTfulWebservice>
<Process>```

#Participate 
If anyone with JAVA knoweledge would like to participate, please drop me an e-mail with subject 'etl project':
ali.mohammad@fcih.net


#TODO
1. Add SOAP Webservice.
2. Add RESTful Webservice.
3. Add UI to handle the etl proceess.
