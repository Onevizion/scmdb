FROM public.ecr.aws/amazoncorretto/amazoncorretto:21-al2023-headless

COPY scmdb.jar /scmdb.jar
CMD ["java", "-jar", "/scmdb.jar"]
