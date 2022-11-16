
"""Tools to do authentication and authorization with JWT."""

import base64
import json
import time
import logging
from jwcrypto import jwt, jwk, jws


def b64_padder(payload: str) -> str:
    if payload is not None:
        payload += '=' * (-len(payload) % 4)
        return payload


def extract_claims(token: str) -> dict:
    enc_claim_text = token.split('.')[1]
    dec_claim_text = base64.b64decode(b64_padder(enc_claim_text))
    claims = json.loads(dec_claim_text)
    return claims


def process_access_token(
    auth_header: str,
    tenant: str,
    check_tenant: bool,
    check_exp: bool,
    tenant_claim_name: str,
    verify_with_secret: bool = None,
 ) -> dict:
    """
    Extract claims, check tenant access, and expiry.

    Parameters
    ----------
    auth_header: string (HTTP header)
    tenant: string

    Returns
    -------
    dict {message, status, user}

    """
    failure_message = {
        'message': 'Access forbidden',
        'status': False,
        'reason': None,
        'claims': None,
    }
    try:
        raw_token = auth_header.split(' ')[1]
        if not verify_with_secret:
            claims = extract_claims(raw_token)
        else:
            k = {'k': verify_with_secret, 'kty': 'oct'}
            key = jwk.JWK(**k)
            token = jwt.JWT(algs=['HS256'])
            try:
                token.deserialize(raw_token, key=key)
                claims = json.loads(token.claims)
            except jwt.JWTExpired as e:
                return failure_message
            except jws.InvalidJWSSignature as e:
                return failure_message
    except Exception as e:
        logging.error(e.message)
        failure_message['reason'] = e.message
        return failure_message
    if check_tenant and claims[tenant_claim_name] != tenant:
        logging.error('Access denied to tenant: %s != %s ', claims[tenant_claim_name], tenant)
        return failure_message
    if check_exp and int(time.time()) > int(claims['exp']):
        logging.error('JWT expired')
        return failure_message
    return {'message': 'OK', 'status': True, 'claims': claims}
