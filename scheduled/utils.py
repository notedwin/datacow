from infisical_client import ClientSettings, GetSecretOptions, InfisicalClient


def get_secret(secret_name):
    client = InfisicalClient(
        ClientSettings(
            access_token="st.bf65884d-fc7e-4c29-8fee-97f9f7d78ecf.0e799730d07add0a48217e2e1ae95705.0a10758bb6796d3cd23222e783f6ac9f",
            site_url="http://192.168.0.200:8080",
        )
    )

    return client.getSecret(
        options=GetSecretOptions(
            environment="prod",
            project_id="d62f85ea-2258-45ae-afa2-857ece8d8743",
            secret_name=secret_name,
        )
    ).secret_value
