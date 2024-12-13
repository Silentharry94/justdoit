import json
import time

import boto3
import uvicorn
from botocore.client import BaseClient
from fastapi import Depends
from linker_atom.api.app import get_app
from linker_atom.api.base import UdfAPIRoute
from linker_atom.config import settings
from pydantic import BaseModel
from starlette.requests import Request
from uvicorn.loops.auto import auto_loop_setup

app = get_app()


@app.on_event("startup")
async def startup_event():
    app.state.runtime_client = boto3.client(
        service_name='sagemaker-runtime',
        region_name='cn-northwest-1',
        aws_access_key_id='',
        aws_secret_access_key=''
    )


async def get_runtime_client(request: Request) -> BaseClient:
    return request.app.state.runtime_client


class RunEndpointBody(BaseModel):
    endpoint_name: str
    body: dict
    content_type: str = 'application/json'


# 实例化路由
route = UdfAPIRoute()


@route.post('/run_endpoint')
def run_endpoint(body: RunEndpointBody, runtime_client: BaseClient = Depends(get_runtime_client)):
    start_time = time.perf_counter()
    response = runtime_client.invoke_endpoint(
        EndpointName=body.endpoint_name,
        ContentType=body.content_type,
        Body=json.dumps(body.body)
    )
    ende_time = time.perf_counter()
    took = ende_time - start_time
    result = json.loads(response['Body'].read().decode())

    return {'took': took, 'code': 0, 'result': result}


# 添加路由
app.include_router(
    router=route,
    prefix=settings.atom_api_prefix,
)


# 运行服务
def run():
    auto_loop_setup(True)
    uvicorn.run(
        app='server:app',
        host='0.0.0.0',
        port=8000,
        workers=settings.atom_workers,
        access_log=False,
    )


if __name__ == '__main__':
    run()