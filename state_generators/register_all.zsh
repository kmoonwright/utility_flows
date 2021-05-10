# prefect register --path ./1_random_state_generator.py --project "State Generators"
# prefect register --path ./2_prev_run_state_changer.py --project "State Generators"
# prefect register --path ./3_flow_run_generator.py --project "State Generators"

python 1_random_state_generator.py
python 2_prev_run_state_changer.py
python 3_flow_run_generator.py