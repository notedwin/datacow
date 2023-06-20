# import mintapi
# import os
# from dotenv import load_dotenv
# load_dotenv()

# email = os.getenv(key="MINT_EMAIL")
# password = os.getenv(key="MINT_PASSWORD")
# totp = os.getenv(key="MINT_TOTP")

# mint = mintapi.Mint(
#     email,
#     password,
#     mfa_method='soft-token',
#     mfa_token=totp,
#     headless=False,
# )

# data = mint.get_account_data()
# print(data)