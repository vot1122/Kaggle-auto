# web test results

| check | path | status | ms | type | result |
|---|---|---|---|---|---|
| landing page | `/` | 200 | 830 | text/html | PASS — <!DOCTYPE html> <html lang="en"> <head> <meta charset="UTF-8"> <meta name="viewport" content="width=device-width, initia |
| file manager page | `/app/files` | 200 | 419 | text/html | PASS — <!DOCTYPE html> <html lang="en"> <head> <meta charset="UTF-8"> <meta name="viewport" content="width=device-width, initia |
| pin api no gid | `/app/files/torrent` | 200 | 358 | application/json | PASS — {"files":[],"engine":"","error":"GID is missing","message":"GID not specified"} |
| pin api invalid gid | `/app/files/torrent?gid=!!!&pin=1234` | 200 | 364 | application/json | FAIL — {"files":[],"engine":"","error":"Invalid GID","message":"Invalid GID"} |
| pin api bad pin | `/app/files/torrent?gid=test123&pin=99` | 200 | 365 | application/json | FAIL — {"files":[],"engine":"","error":"Invalid pin","message":"The PIN you entered is incorrect. Try Again!"} |
| qbit ui valid pass | `/qbit/?pass=52731cc8ab38b88410994a20` | 200 | 366 | text/html | PASS — <!DOCTYPE html> <html lang="C"> <head> <meta charset="UTF-8" /> <meta http-equiv="X-UA-Compatible" content="IE=10" /> <m |
| qbit api valid pass | `/qbit/api/v2/app/version?pass=52731cc8ab38b88410994a20` | 200 | 399 | text/plain | PASS — v4.4.1 |
| qbit ui wrong pass | `/qbit/?pass=wrongpass` | 403 | 387 | text/html | PASS — <h1>403: Unauthorized access</h1> |
| qbit api no pass | `/qbit/api/v2/app/version` | 403 | 352 | text/html | PASS — <h1>403: Unauthorized access</h1> |
| nzb ui valid pass | `/nzb/?pass=c23d8198078c635d61cd5ad4` | 500 | 351 | text/html | FAIL — <h1>500: Internal server error</h1> |
| nzb api valid pass | `/nzb/api?mode=version&output=json&pass=c23d8198078c635d61cd5ad4` | 500 | 361 | text/html | FAIL — <h1>500: Internal server error</h1> |
| nzb api wrong pass | `/nzb/api?mode=version&output=json&pass=wrongpass` | 403 | 620 | text/html | PASS — <h1>403: Unauthorized access</h1> |
| nzb login page | `/nzb/login` | 403 | 350 | text/html | PASS — <h1>403: Unauthorized access</h1> |
| stream bogus token | `/stream/bogus1234` | 404 | 535 | text/plain | PASS — unknown link |
| download bogus token | `/dl/bogus1234` | 404 | 469 | text/plain | PASS — unknown link |
| poster bogus token | `/poster/bogus1234` | 404 | 737 | text/html | PASS — <h1>404: No artwork</h1> |
| tracks bogus token | `/tracks/bogus1234` | 404 | 352 | application/json | PASS — {"detail":"Not Found"} |
| path traversal guard | `/nzb/../etc/passwd` | 404 | 361 | application/json | PASS — {"detail":"Not Found"} |
| unknown page 404 | `/definitely-not-a-page` | 404 | 316 | application/json | PASS — {"detail":"Not Found"} |

**15 pass / 4 fail / 0 error**
