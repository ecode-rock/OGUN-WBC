@echo off
echo ==============================
echo WBC Data Updater
echo ==============================

cd C:\OGUN\WBC

echo.
echo Step 1 - Loading new games into Supabase...
python load_wbc.py

echo.
echo Step 2 - Pushing WBC_GAMES.md to GitHub...
git add WBC_GAMES.md
git commit -m "Update WBC_GAMES.md with new completed games"
git push

echo.
echo ==============================
echo Done! App is updated.
echo ==============================
pause