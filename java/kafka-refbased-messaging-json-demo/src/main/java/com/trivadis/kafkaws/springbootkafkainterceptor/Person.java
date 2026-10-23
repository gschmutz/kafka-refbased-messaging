package com.trivadis.kafkaws.springbootkafkainterceptor;

/**
 * Plain JSON payload DTO for the JSON reference-based-messaging demo.
 * <p>
 * Unlike the Avro demo (which uses a generated {@code com.trivadis.demo.dto.Person}), this is an
 * ordinary POJO serialized with Spring Kafka's {@code JsonSerializer} / {@code JsonDeserializer}.
 * The serializer-agnostic header interceptors treat it as an opaque value, so no Avro or Schema
 * Registry is involved.
 */
public class Person {
    private String id;
    private String firstName;
    private String middleName;
    private String lastName;
    private String street;
    private String number;
    private String zipCode;
    private String city;

    public Person() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getMiddleName() {
        return middleName;
    }

    public void setMiddleName(String middleName) {
        this.middleName = middleName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", firstName='" + firstName + '\'' +
                ", middleName='" + middleName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", street='" + street + '\'' +
                ", number='" + number + '\'' +
                ", zipCode='" + zipCode + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
