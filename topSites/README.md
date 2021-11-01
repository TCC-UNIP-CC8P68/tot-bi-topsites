sudo docker build -t tot-bi-topsites:0.0.1 .
sudo docker run -d --network=host tot-bi-topsites:0.0.1
