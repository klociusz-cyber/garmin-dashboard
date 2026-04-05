@echo off
cd /d "%~dp0"

echo ============================================
echo  Garmin Sync
echo ============================================
echo.

REM Znajdź dysk Garmina automatycznie
set GARMIN_DRIVE=

for %%D in (D E F G H I J K L M N O P Q R S T U V W X Y Z) do (
    if exist "%%D:\Garmin\Activity\" (
        set GARMIN_DRIVE=%%D
        goto :found
    )
)

echo BLAD: Nie znaleziono zegarka Garmin.
echo Upewnij sie ze zegarek jest podlaczony przez USB.
pause
exit /b 1

:found
echo Znaleziono Garmin na dysku: %GARMIN_DRIVE%:\
echo.

REM Kopiuj pliki aktywnosci (.fit)
echo [1/4] Kopiuje aktywnosci...
xcopy "%GARMIN_DRIVE%:\Garmin\Activity\*.fit" "fit_files\" /D /Y /Q
echo.

REM Kopiuj pliki monitoringowe (kroki, tetno dzienne)
echo [2/4] Kopiuje dane monitoringowe...
if not exist "monitor_files\" mkdir "monitor_files\"
xcopy "%GARMIN_DRIVE%:\Garmin\Monitor\*.fit" "monitor_files\" /D /Y /Q
echo.

REM Importuj aktywnosci do bazy
echo [3/4] Importuje aktywnosci do bazy danych...
python parser\parse_fit.py fit_files\
echo.

REM Importuj dane monitoringowe do bazy
echo [4/4] Importuje dane monitoringowe...
python parser\parse_monitor.py monitor_files\
echo.

echo ============================================
echo  Gotowe! Odswierz dashboard: http://localhost:8501
echo ============================================
pause
