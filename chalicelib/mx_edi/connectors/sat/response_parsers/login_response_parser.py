import xmltodict

from .response_parser import ResponseParser


class LoginParser(ResponseParser):
    @staticmethod
    def parse(response: str) -> dict[str, str]:
        """Gets the token from the raw response"""
        response_dict = xmltodict.parse(response)
        return {
            "created": response_dict["Envelope"]["Header"]["Security"]["Timestamp"]["Created"],
            "expires": response_dict["Envelope"]["Header"]["Security"]["Timestamp"]["Expires"],
            "token": response_dict["Envelope"]["Body"]["AutenticaResponse"]["AutenticaResult"],
        }
