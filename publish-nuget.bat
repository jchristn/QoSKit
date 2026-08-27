@echo off
setlocal enabledelayedexpansion

rem ---------------------------------------------------------------------------
rem  publish-nuget.bat <NuGet-API-key>
rem
rem  Packs the QoSKit packages in Release and pushes them to nuget.org. Each
rem  .nupkg is pushed with its matching .snupkg symbol package alongside it
rem  (dotnet pushes the symbol package automatically when it sits next to the
rem  main package).
rem ---------------------------------------------------------------------------

if "%~1"=="" (
    echo Usage: publish-nuget.bat ^<NuGet-API-key^>
    exit /b 1
)

set "APIKEY=%~1"
set "SOURCE=https://api.nuget.org/v3/index.json"
set "CONFIG=Release"

pushd "%~dp0"

echo.
echo === Packing (%CONFIG%) ===
dotnet pack src\QoSKit\QoSKit.csproj -c %CONFIG%
if errorlevel 1 goto :error
dotnet pack src\QoSKit.Persistence.Sqlite\QoSKit.Persistence.Sqlite.csproj -c %CONFIG%
if errorlevel 1 goto :error

echo.
echo === Pushing to %SOURCE% ===
dotnet nuget push "src\QoSKit\bin\%CONFIG%\QoSKit.*.nupkg" --api-key %APIKEY% --source %SOURCE% --skip-duplicate
if errorlevel 1 goto :error
dotnet nuget push "src\QoSKit.Persistence.Sqlite\bin\%CONFIG%\QoSKit.Persistence.Sqlite.*.nupkg" --api-key %APIKEY% --source %SOURCE% --skip-duplicate
if errorlevel 1 goto :error

echo.
echo === Done. Packages and symbols published. ===
popd
endlocal
exit /b 0

:error
echo.
echo === Publish FAILED (exit code %errorlevel%). ===
popd
endlocal
exit /b 1
