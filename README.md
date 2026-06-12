# -KafkaSpringBootDOC
Kafka Spring Boot Documentation

STREAM A TO STREAM B JOIN 
INNER JOIN
<img width="1693" height="929" alt="image" src="https://github.com/user-attachments/assets/47272426-8b6a-4703-a24b-1a4574613a6d" />

LEFT JOIN 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/d6a19f10-919a-40aa-9f00-1d1a0c7f2977" />


OUTER JOIN 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/798b2707-b173-408e-b25a-d8b5193b3fab" />

Given:
Stream A keys: 1, 2, 4  
Stream B keys: 1, 2, 5  

| Join Type | Output keys | Explanation |
|-----------|-------------|-------------|
| Inner     | 1, 2        | Keys present in both A and B. 4 and 5 are dropped. |
| Left      | 1, 2, 4     | All A keys. 4 has no match → right side = null. |
| Outer     | 1, 2, 4, 5  | All keys from A and B. 4 → right null; 5 → left null. |.

ASYMETRIC INNER JOIN 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/33b3d30b-19cf-4eb5-b72c-75d57f83c743" />


WINDOWED INNER JOIN WITH GRACE 
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/9038b30e-bb58-43ec-a6bb-aef872c31820" />
