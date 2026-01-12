from typing import List
from fastapi.responses import FileResponse
from fastapi import APIRouter, UploadFile, File, Request, Depends, HTTPException
from app.services import ImageService
from app.models import UploadRequest, UploadResponse, StatusResponse, DownloadResponse

upscale_v1 = APIRouter()

def get_image_service(request: Request) -> ImageService:
    return request.app.state.image_service

@upscale_v1.post("/upload", response_model=UploadResponse)
async def upload_photo(
        file: UploadFile = File(...),
        scale: int = 2,
        model_name: str = "image_scale_l_x2",
        model_version: str = "1",
        image_service: ImageService = Depends(get_image_service)
):
    try:
        validated_data = UploadRequest(scale=scale)
        return await image_service.upload_photo(file, validated_data.scale, model_name, model_version)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@upscale_v1.post("/downloadImages")
async def download_images(guids: List[str], image_service: ImageService = Depends(get_image_service)):
    try:
        if len(guids) == 1:
            return await image_service.download_image(guids[0])
        else:
            return await image_service.download_images(guids)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

