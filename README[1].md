# Kotak Desk market terminal - put it on the internet

This folder is ready to host on Render's free plan. You need a free GitHub account and a
free Render account. Nothing is installed on your PC.

## 1. Upload these files to GitHub
1. github.com -> New repository -> name it `markets` -> Create.
2. "uploading an existing file" -> drag in **all five files from this folder**
   (`mini_bloomberg_web.py`, `wsgi.py`, `requirements.txt`, `render.yaml`, `README.md`) -> Commit.

## 2. Create the website on Render
1. render.com -> sign up with GitHub.
2. New -> Blueprint -> pick your `markets` repository -> Connect.
3. Render asks for `TERMINAL_PASSWORD`. Type a long password you will share with friends.
   Friends log in with user name `friend` and that password.
4. Apply. The first build takes about 5 minutes.
5. Your site opens at `https://marketsbyaniruddha.onrender.com`
   (Render adds letters to the name if someone already took it; the real address is on the service page).

The free plan puts the site to sleep after 15 minutes without visitors. The next visit takes
about a minute to wake it up, then data fills in over the next few seconds.

## 3. Use your own domain (optional)
1. Buy the domain at a registrar (search for `marketsbyaniruddha.com` there to see if it is free).
2. Render -> your service -> Settings -> Custom Domains -> Add `www.marketsbyaniruddha.com` -> Save.
3. At your registrar's DNS settings add the record Render shows (a CNAME for `www` pointing to your
   `...onrender.com` address). Delete any AAAA records for the domain.
4. Back on Render click Verify. HTTPS is added automatically. The bare `marketsbyaniruddha.com`
   redirects to `www` on its own.

## Notes
- To make the site public with no login, delete the `TERMINAL_PASSWORD` setting on Render.
- To change the password later: Render -> service -> Environment -> edit `TERMINAL_PASSWORD`.
- Data sources (NSE, TradingView, Investing.com and others) may block or limit traffic from cloud
  servers, so some tabs can show "data temporarily unavailable" online even if they work on your PC.
- Those sources have their own terms of use. Check them before making the page open to everyone.
