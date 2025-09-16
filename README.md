Even though the vector tap tool already supports connecting remotely, this is intended to be even lighter weight. 

Vector version: vector 0.49.0


Install vector

Start it with this config opening up tcp/8080 if neccesary

/opt/vector/bin/vector --config /opt/vector/config/vector.yaml

if you know what conda is, use the conda_vector_env.yaml. Otherwise just pip install the packages in there.

run: nose2 test_vector_config
