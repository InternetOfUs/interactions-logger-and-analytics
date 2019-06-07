# Memorable Experience - Logging and performances

### Version 0.0.1
- Logging end-points can be tested @ /LogMessage AND /LogMessages 
- Performance APIs designed
- Elasticsearch connected and 

### Version 0.0.2
- Parametrized parameters -hs -p for the Elasticsearch connection 
- docker-support for the deployment
- nginx configuration 
- running end-points @ memex.u-hopper.com/logging/LogMessage AND memex.u-hopper.com/logging/LogMessages 
- Utils class initialized

### Version 0.0.3
- Parametrized Elasticsearch instance 
- restfull flask introduced 
- indexes rename

### Version 0.0.4
- endpoints alignment
- various adjustments
- alpha release @ memex.u-hopper.com

### Version 0.0.5
- minor adjustments
- default value of the project is memex. Whenever a message without the project value arrives, it is assigned to memex automatically
- utility package to use the APIs is now part of the release. The utilities can be found in `memex_logging\memex_logging_lib\logging_utils.py` while the file `example.py` contains a usage example

### Version 0.0.6 
- new data models for messages 
- new data models for logging
- alpha version data model of analytics 
- end-point for the documentation 
- new version of the messages APIs