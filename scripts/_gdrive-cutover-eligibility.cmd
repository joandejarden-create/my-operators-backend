@echo off
if exist "C:\Dev\Nightly-Backups" (echo Nightly-Backups=YES) else (echo Nightly-Backups=NO)
if exist "C:\Dev\Backup-Staging\2026-08-22_0200" (echo Aug22=YES) else (echo Aug22=NO)
if exist "C:\Dev\Backup-Staging\2026-10-01_0200" (echo Oct1=YES) else (echo Oct1=NO)
if exist "C:\Dev\Cursor-Recovery-Archive-2026-09-08" (echo CursorRecovery=YES) else (echo CursorRecovery=NO)
if exist "C:\Dev\Backup-Scripts\dealality-snapshots" (echo Snapshots=YES) else (echo Snapshots=NO)
if exist "C:\Dev\dealality-backups\LATEST\deal-capture-proxy\.env" (echo env=YES) else (echo env=NO)
if exist "C:\Dev\dealality-backups\LATEST\deal-capture-proxy\.env.local" (echo env.local=YES) else (echo env.local=NO)
if exist "C:\Dev\dealality-backups\LATEST\deal-capture-proxy\.env.strix" (echo env.strix=YES) else (echo env.strix=NO)
if exist "C:\Dev\dealality-backups\LATEST\deal-capture-proxy\matcha\.env" (echo matcha.env=YES) else (echo matcha.env=NO)
if exist "C:\Dev\dealality-backups\LATEST\deal-capture-proxy\rail-explore\.env" (echo rail.env=YES) else (echo rail.env=NO)
dir /b "G:\My Drive\Dealality Backups"
