# FPL_checker
This data pipeline, takes data from an API that checks each player in the league and see how well they are doing and how their ranking is affected by their points and how they do in the weekly basis, it also checks where each player is from and we can see how their favorite team

Steps:
First Step: we would need to orchestrate our services and start running them, we can do that by creating a docker file:

To begin we would need to start by creating an airflow image, donwloading the requirements.txt and creating dags folder and dont forget to create a Dockerfile, commands for it is:
mkdir dags/
docker-compose -f docker-compose.yaml up -d

Second Step: is to set up the kafka server using the second docker compose file and run this command to set it up in docker:
docker-compose -f docker-compose2.yaml up -d

Third Step: to copy the dag from our working directory to the dags folder using the command:
cp dag.py dags/

Fourth Step: we would need to create a topic under topics tab in our kafka server and we can use this URL to access it: http://localhost:8888/

Fifth Step: Make sure to have a snowflake account and should have the FPL warehouse created under admin and the schema in data is created under data

Sixth Step: we go to our airflow server and run the dag from there and we can access it using this URL: http://localhost:8089/

Seventh Step: then we would validate that the data is being filled in our snowflake tables in the UI and we can see our kafka message being filled with data coming from the API and we can validate that by going to UI: http://localhost:8888/

Eighth Step: to connect to PowerBi to start the creation of graphs and we can do that by going to open->get data-> database -> snowflake and then we would need to add the server ID and the warehouse name. or you can import the report I create and go from there.

Author and Coder: Hamdan Kasem

