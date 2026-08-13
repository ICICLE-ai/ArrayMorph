import os
import cv2
import numpy as np

# Paths
input_path = r'/fs/ess/PAS2699/crdean95/Rear Implement Camera/' 
output_path = r'/fs/ess/PAS2699/rectified_images/'  # The folder to save processed images


os.makedirs(output_path, exist_ok=True)

points = [(2900, 400), (3700, 1300), (200, 1200), (1000, 400)]

# Function to extract quadrilateral from the image

def extract_quadrilateral(image, points):
    pts = np.array(points, dtype='float32')
    mask = np.zeros(image.shape[:2], dtype=np.uint8)
    cv2.fillPoly(mask, [pts.astype(int)], 255)
    extracted = cv2.bitwise_and(image, image, mask=mask)
    return extracted

# Function to transform the extracted quadrilateral to a rectangle

def transform_to_rectangle(image, points):
    pts = np.array(points, dtype='float32')
    width = int(max(np.linalg.norm(pts[0] - pts[1]), np.linalg.norm(pts[2] - pts[3])))
    height = int(max(np.linalg.norm(pts[0] - pts[3]), np.linalg.norm(pts[1] - pts[2])))
    dst = np.array([
        [0, 0],
        [width - 1, 0],
        [width - 1, height - 1],
        [0, height - 1]
    ], dtype='float32')
    M = cv2.getPerspectiveTransform(pts, dst)
    rectified = cv2.flip(cv2.transpose(cv2.warpPerspective(image, M, (width, height))), 1)
    return rectified

# Process each subfolder (like GX010060, GX010059) inside 'Rear Implement Camera'

subfolder_list = os.listdir(input_path)
for subfolder in subfolder_list:
    subfolder_path = os.path.join(input_path, subfolder)
    
    if os.path.isdir(subfolder_path):  
        # Create the corresponding folder inside 'rectified_images'
        output_subfolder_path = os.path.join(output_path, subfolder)
        os.makedirs(output_subfolder_path, exist_ok=True)
        
        
        img_list = os.listdir(subfolder_path)
        for img_name in img_list:
            if img_name.endswith('.JPEG'):  
                image = cv2.imread(os.path.join(subfolder_path, img_name))
                
                # Extract quadrilateral and transform to rectangle
                extracted_quad = extract_quadrilateral(image, points)
                rectified_image = transform_to_rectangle(extracted_quad, points)
                
                # Save the processed image in the corresponding subfolder inside 'rectified_images'
                output_filename = f"rectified_{img_name}"
                cv2.imwrite(os.path.join(output_subfolder_path, output_filename), rectified_image)
