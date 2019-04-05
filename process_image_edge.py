import ast
import json
import os
import tempfile
import uuid
from time import gmtime, strftime

import boto3
from PIL import Image
from elasticsearch import Elasticsearch

from utility import logger

ES_HOST = "search-od-happy-face-5i3vty7clvmiz3cvgus3n6dfpa.us-east-1.es.amazonaws.com"
ES_INDEX_NAME = "test"
GROUP_PHOTOS = "group_snaps"
REGION_NAME = 'us-east-1'
DYNAMO_TBL_FACEGROUPS = 'FaceGroupsEdge'

rek_client = boto3.client('rekognition', region_name=REGION_NAME)
s3_client = boto3.client('s3', region_name=REGION_NAME)
s3 = boto3.resource('s3', region_name=REGION_NAME)
dynamo_client = boto3.client('dynamodb', region_name=REGION_NAME)
dynamodb = boto3.resource('dynamodb', REGION_NAME)

s3_bucket = 'image-recognition-edge'
bucket = s3.Bucket(s3_bucket)

tmp = "{}/happyface".format(tempfile.gettempdir())
x, y = 2592, 1944
similarity_q = 50.00


def process_image(tmp_path):

    try:
        with open(tmp_path, "rb") as imageFile:
            f = imageFile.read()
            b = bytearray(f)
    except Exception as e:
        logger.error('Error occurred while opening image file: {}'.format(str(e)))
        raise Exception('Error occurred while opening image file: ', e)

    try:
        group_s3_path = "{}/{}".format(GROUP_PHOTOS, os.path.basename(tmp_path))
        logger.info("Uploading photo : " + group_s3_path)
        s3_client.upload_file(tmp_path, s3_bucket, group_s3_path, ExtraArgs={'ACL': 'public-read'})
        s3.ObjectAcl(s3_bucket, group_s3_path).put(ACL='public-read')
        response = rek_client.detect_faces(
            Image={
                'Bytes': b
            },
            Attributes=[
                'ALL',
            ]
        )
        for index, image in enumerate(response['FaceDetails']):
            path = os.path.join(tmp, os.path.basename(tmp_path).rstrip("/"))
            img = Image.open(path)
            width, height, left, top = image['BoundingBox']['Width'], \
                                       image['BoundingBox']['Height'], \
                                       image['BoundingBox']['Left'], \
                                       image['BoundingBox']['Top']
            left, top, right, bottom = left * x, top * y, (left + width) * x, (height + top) * y
            cropped = img.crop((left, top, right, bottom))
            cropped = cropped.resize((100, 100), Image.ANTIALIAS)

            dir_path = os.path.join(tmp, os.path.basename(tmp_path).split(".")[0])



            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
            thum_uuid = str(uuid.uuid4())
            thumbnail_path = os.path.join(dir_path, thum_uuid + ".jpeg")
            cropped.save(thumbnail_path)
            s3_thumbnail_path = "thumbnail/{}".format(
                os.path.basename(tmp_path).split(".")[0] + "_" + os.path.basename(thumbnail_path))



            happy_emotion = {'Type': 'HAPPY', 'Confidence': 0.0}
            for emotion in image['Emotions']:
                # if emotion['Type'] == 'HAPPY' and emotion['Confidence'] > 90:
                if emotion['Type'] == 'HAPPY':
                    happy_emotion = emotion

                    # upload thumbnail to s3
                    logger.info("Uploading thumbnail: " + s3_thumbnail_path)
                    s3_client.upload_file(thumbnail_path, s3_bucket, s3_thumbnail_path, ExtraArgs={'ACL': 'public-read'})
                    s3.ObjectAcl(s3_bucket, s3_thumbnail_path).put(ACL='public-read')
		    os.remove(thumbnail_path)
		    os.rmdir(dir_path)

                    # Compare with existing thumnails and store data in FaceGroups
                    compare_existing_thumbnails(s3_thumbnail_path, happy_emotion)

    except Exception as e:
        logger.error('Error occurred while detecting faces: {}'.format(str(e)))
        raise Exception('Error occurred while detecting faces:', e)


def compare_existing_thumbnails(thumbnail_path, emotions):
    s3_thumbnail_path = os.path.join("https://s3.amazonaws.com", s3_bucket, thumbnail_path)
    face_group_result = get_face_group_data()
    now = str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))

    if face_group_result['Items']:
        is_face_matched = False
        for i in face_group_result['Items']:
            json_str = json.dumps(i)
            resp_dict = json.loads(json_str)
            target_image_list = ast.literal_eval(resp_dict['image_list'])
            target_path = "thumbnail" + target_image_list[0]['path'].split("thumbnail")[1]
            if target_path != thumbnail_path:
                img_uuid = resp_dict['uuid']
                compare_response = compare_faces(thumbnail_path, target_path)
                if compare_response['FaceMatches']:
                    for dictionary in compare_response['FaceMatches']:
                        if float(dictionary['Similarity']) > similarity_q:
                            t_dict = dict(path=s3_thumbnail_path, emotions=emotions, date=now)
                            target_image_list.append(t_dict)
                            save_face_group_data(img_uuid, target_image_list)
                            push_data_to_es(img_uuid, [t_dict])
                            is_face_matched = True
                            break

        if not is_face_matched:
            img_uuid = str(uuid.uuid4())
            image_list = [dict(path=s3_thumbnail_path, emotions=emotions, date=now)]
            save_face_group_data(img_uuid, image_list)
            push_data_to_es(img_uuid, image_list)
    else:
        img_uuid = str(uuid.uuid4())
        image_list = [dict(path=s3_thumbnail_path, emotions=emotions, date=now)]
        save_face_group_data(img_uuid, image_list)
        push_data_to_es(img_uuid, image_list)


def save_face_group_data(img_uuid, image_list):
    try:
        response = dynamo_client.put_item(
            Item={
                'uuid': {
                    'S': img_uuid,
                },
                'image_list': {
                    'S': str(image_list),
                },
            },
            ReturnConsumedCapacity='TOTAL',
            TableName=DYNAMO_TBL_FACEGROUPS,
        )
    except Exception as e:
        logger.error('Error occurred while storing face data: {}'.format(str(e)))
        raise Exception('Error occurred while storing face data:', e)


def get_face_group_data():
    try:
        table = dynamodb.Table(DYNAMO_TBL_FACEGROUPS)
        response = table.scan()
    except Exception as e:
        logger.error('Error occurred fetching face data from dynammodb: {}'.format(str(e)))
        raise Exception('Error occurred fetching face data from dynammodb:', e)

    return response



def compare_faces(src_image, target_image):
    # logger.info("src_image: ", src_image, "target_image: ", target_image)
    try:
        response = rek_client.compare_faces(
            SourceImage={
                'S3Object': {
                    'Bucket': s3_bucket,
                    'Name': src_image
                }
            },
            TargetImage={
                'S3Object': {
                    'Bucket': s3_bucket,
                    'Name': target_image
                }
            }
        )
        return response
    except Exception as e:
        logger.error('Error occurred while comparing faces using rekognition : {}'.format(str(e)))
        raise Exception('Error occurred while comparing faces using rekognition: ', e)


def get_result():
    face_group_result = get_face_group_data()
    result_dict_list = []
    if face_group_result['Items']:
        for i in face_group_result['Items']:
            json_str = json.dumps(i)
            resp_dict = json.loads(json_str)
            target_image_list = ast.literal_eval(resp_dict['image_list'])
            sum = 0
            max_confidence = 0
            happy_path = None
            for image_dict in target_image_list:
                confidence = image_dict['emotions']['Confidence']
                if max_confidence < confidence:
                    max_confidence = confidence
                    happy_path = image_dict['path']
                sum += confidence
            average_confidence = sum / len(target_image_list)
            result_dict_list.append(dict(image=happy_path, confidence=average_confidence))

    store_result_to_db(result_dict_list)
    return result_dict_list


def store_result_to_db(result_dict_list):
    for res_dict in result_dict_list:
        try:
            response = dynamo_client.put_item(
                Item={
                    'path': {
                        'S': res_dict['image'],
                    },
                    'confidence': {
                        'N': str(res_dict['confidence']),
                    },
                },
                ReturnConsumedCapacity='TOTAL',
                TableName='Results',
            )
        except Exception as e:
            logger.error('Error occurred while storing image {} result data : {}'.format(res_dict['image'], str(e)))
            raise Exception('Error occurred while storing image {} result data : {}'.format(res_dict['image'], str(e)))


def get_es():
    es = None
    try:
        es = Elasticsearch([{"host": ES_HOST, "port": 80}])
    except Exception as e:
        raise Exception('Error while creating Elasticsearch client', e)
    if not es:
        raise Exception('Error while creating Elasticsearch client', e)
    return es


def push_data_to_es(img_uuid, img_list):
    logger.info("Adding data to index..")
    best_path = img_list[0]['path']
    for img in img_list:
        item_uuid = str(uuid.uuid4())
        logger.info(img["date"].split(".")[0])
        es_dict = dict(path=img['path'],
                       confidence=img['emotions']['Confidence'],
                       best_path=best_path,
                       date=img['date'],
                       image_uuid=img_uuid)
        res = get_es().index(index=ES_INDEX_NAME, doc_type='result', id=item_uuid, body=es_dict)
        logger.info(res['result'])


def push_data_to_final_result_es():
    doc = get_result_from_db()
    for d in doc['Items']:
        logger.info("Adding result data to index..", type(d))
        item_uuid = str(uuid.uuid4())
        es_dict = dict(s3_path=d['path'], confidence=d['confidence'])
        res = get_es().index(index=ES_INDEX_NAME, doc_type='final_result', id=item_uuid, body=es_dict)
        logger.info(res['result'])


def get_result_from_db():
    try:
        table = dynamodb.Table('Results')
        response = table.scan()
    except Exception as e:
        logger.error('Error occurred fetching result data from dynammodb: {}'.format(str(e)))
        raise Exception('Error occurred fetching result data from dynammodb:', e)

    return response
