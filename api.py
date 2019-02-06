import responder

import get

api = responder.API(cors=True, cors_params={"allow_origins": ["*"]})


@api.route("/{query}")
def root(req, resp, query):
    resp.media = get.by_company_name(query)


if __name__ == "__main__":
    api.run()
