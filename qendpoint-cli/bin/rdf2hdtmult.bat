@echo off

call "%~dp0\javaenv.bat"

"%JAVACMD%" -XX:NewRatio=1 -XX:SurvivorRatio=9 %JAVAOPTIONS% -classpath %~dp0\..\lib\* com.the_qa_company.qendpoint.core.tools.RDF2HDTMult %*
