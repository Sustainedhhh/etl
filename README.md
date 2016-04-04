## etl
Project to implement ETL (Extract, Transform and Load) concept with JAVA 
The idea in a simple way, we want to create processes which allow multiple sub-processes with input (database query, SOAP webservice, RESTful webservice, CSV file or flat files)

## Sample
``` <Process>
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
<Process> 
```

## Participate 
If anyone with JAVA knoweledge would like to participate, please drop me an e-mail with subject 'etl project':
ali.mohammad@fcih.net


## TODO
1. Add SOAP Webservice.
2. Add RESTful Webservice.
3. Add UI to handle the etl proceess.


## License
    Copyright 2015-2019 dinus

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
