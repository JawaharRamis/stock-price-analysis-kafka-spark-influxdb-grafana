import os
from dotenv import load_dotenv

def load_environment_variables(env_file_path=".env"):
    """
    Load environment variables from a .env file or system environment.
    
    Args:
        env_file_path (str, optional): Path to the .env file. Defaults to ".env".

    Returns:
        dict: A dictionary of environment variable names and their values.
    """
    # Load environment variables from the .env file (if it exists)
    if os.path.exists(env_file_path):
        load_dotenv(env_file_path)

    env_vars = {}
    for key, value in os.environ.items():
        env_vars[key] = value

    return env_vars

# Example usage:
env_vars = load_environment_variables()

# Access specific environment variables
stocks = env_vars.get("STOCKS")
kafka_topic_stock_info = env_vars.get("STOCK_GEN_INFO_KAFKA_TOPIC")
kafka_topic_stock_prices = env_vars.get("STOCK_PRICE_KAFKA_TOPIC")

