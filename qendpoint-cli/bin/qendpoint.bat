@echo off

call "%~dp0\javaenv.bat"

"%JAVACMD%" %JAVAOPTIONS% -classpath %~dp0\..\lib\* org.springframework.boot.loader.JarLauncher %*
