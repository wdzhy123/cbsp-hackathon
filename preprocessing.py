from bravado.client import SwaggerClient
import pandas as pd

cbioportal = SwaggerClient.from_url('https://www.cbioportal.org/api/api-docs',
                                    config={"validate_requests": False, "validate_responses": False}
                                    )


def main():
    ds = pd.DataFrame(columns=["studyId", "patientId", "OS_MONTHS", "OS_STATUS", "cancerTypeId"])
    studies = cbioportal.Studies.getAllStudiesUsingGET().result()
    study_list = [i['studyId'] for i in studies]
    n = len(study_list)
    m = 0
    for i in study_list:
        m = m + 1
        print("start processing study {} ({}/{}), processed {} samples".format(i, m, n, len(ds)))
        os_months = get_clinical_data(i, attributeid="OS_MONTHS")
        os_status = get_clinical_data(i, attributeid="OS_STATUS")
        if len(os_months) == 0 or len(os_status) == 0:
            print("no OS or OS status in this dataset, continue")
            continue
        else:
            print("There are {} patients in this dataset".format(len(os_months)))
            os_months_ds = pd.DataFrame.from_dict([m._asdict() for m in os_months])
            os_status_ds = pd.DataFrame.from_dict([m._asdict() for m in os_status])
            os_months_ds = os_months_ds[['studyId', 'patientId', 'value']]
            os_months_ds = os_months_ds.rename(mapper={'value': 'OS_MONTHS'}, axis='columns')
            os_status_ds = os_status_ds[['patientId', 'value']]
            os_status_ds = os_status_ds.rename(mapper={'value': 'OS_STATUS'}, axis='columns')
            os_ds = pd.merge(os_months_ds, os_status_ds, on='patientId')
            patients = get_patient_data(i)
            patients_ds = pd.DataFrame.from_dict([dict(m._asdict(), **m._asdict()['cancerStudy']) for m in patients])
            patients_ds = patients_ds[['patientId', 'cancerTypeId']]
            study_ds = pd.merge(os_ds, patients_ds, on='patientId')
            ds = pd.concat([ds, study_ds])
        ds.to_csv('patients_with_OS.csv', header=True, index=False, mode='w')
    ds.to_csv('patients_with_OS.csv', header=True, index=False, mode='w')
    print("Complete!")


def get_clinical_data(studyid, attributeid=None, projection='SUMMARY'):
    results = cbioportal.Clinical_Data.getAllClinicalDataInStudyUsingGET(studyId=studyid,
                                                                         projection=projection,
                                                                         clinicalDataType="PATIENT",
                                                                         attributeId=attributeid).result()
    return results


def get_clinical_patient_data(studyid, patientid, attributeid=None, projection='SUMMARY'):
    results = cbioportal.Clinical_Data.getAllClinicalDataOfPatientInStudyUsingGET(studyId=studyid,
                                                                                  projection=projection,
                                                                                  patientId=patientid,
                                                                                  attributeId=attributeid).result()
    return results


def get_patient_data(studyid):
    results = cbioportal.Patients.getAllPatientsInStudyUsingGET(studyId=studyid, projection='DETAILED').result()
    return results


if __name__ == "__main__":
    main()
