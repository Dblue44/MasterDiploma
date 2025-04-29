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
        image_service: ImageService = Depends(get_image_service)
):
    try:
        validated_data = UploadRequest(scale=scale)
        return await image_service.upload_photo(file, validated_data.scale)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@upscale_v1.get("/status/{task_id}", response_model=StatusResponse)
async def get_status(
        task_id: str,
        image_service: ImageService = Depends(get_image_service)
):
    try:
        return await image_service.get_status(task_id)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@upscale_v1.get("/download/{task_id}", response_model=DownloadResponse)
async def download(
        task_id: str,
        image_service: ImageService = Depends(get_image_service)
):
    try:
        file_path, filename = await image_service.download_image(task_id)
        return {"file_path": str(file_path), "filename": filename}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
