@echo off

call "%~dp0\javaenv.bat"

"%JAVACMD%" %JAVAOPTIONS% -classpath %~dp0\..\lib\* com.the_qa_company.qendpoint.core.tools.HDTInfo %*
