import java.io.*;
class GenerateData {

    // A helper function, used to generate random integers.
    private int generateRandom(int min, int max) {
        int random = 0;
        random = (int) Math.round(Math.random() * (max - min)) + min;
        return random;
    }

    // generate name
    private String generateName() {
        StringBuilder result = new StringBuilder();
        int length = generateRandom(10,20);
        for(int i = 0; i < length; i++) {
            int uplow = 0;
            uplow = generateRandom(1,2);
            String character = null;
            switch(uplow) {
                case 1:
                    character = String.valueOf((char)generateRandom(65,90));
                    break;
                case 2:
                    character = String.valueOf((char)generateRandom(97,122));
                    break;
            }
            result.append(character);
        }
        return result.toString();

    }

    // generate age
    private int generateAge() {
        int age = generateRandom(10,70);
        return age;
    }

    // generate gender
    private String generateGender() {
        String gender = null;
        int genderRandom = generateRandom(1,2);
        switch (genderRandom){
            case 1:
                gender = "male";
                break;
            case 2:
                gender = "female";
                break;
        }
        return gender;
    }

    // generate countrycode
    private int generateCountrycode() {
        int countrycode = generateRandom(1,10);
        return countrycode;
    }

    // generate salary
    private float generateSalary() {
        float salary = 0;
        salary = (float) Math.random()* 9900 + 100;
        return salary;
    }

    // file Customers.txt
    private void createCustomers() {
        File file = new File("Customers.txt");
        try {
            FileWriter fw = new FileWriter(file);
            BufferedWriter bufw = new BufferedWriter(fw);
            for(int i = 0 ; i < 50000; i++) {
                String content = String.valueOf(i+1) + "," + generateName() + "," + String.valueOf(generateAge()) + "," + generateGender() + "," + String.valueOf(generateCountrycode()) + "," + String.valueOf(generateSalary());
                bufw.write(content);
                bufw.newLine();
            }
            bufw.close();
            fw.close();
        }catch(Exception e) {
            e.printStackTrace();
        }

    }

    // generate TransID
    private int generateCustID() {
        int custID = generateRandom(1,50000);
        return custID;
    }

    // generate TransTotal
    private float generateTransTotal() {
        float transTotal = 0;
        transTotal = (float) Math.random()* 990 + 10;
        return transTotal;
    }

    // generate TransNumItems
    private int generateNumItem() {
        int numItem = generateRandom(1,10);
        return numItem;
    }

    // generate TransDesc
    private String generateTransDesc() {
        StringBuilder result = new StringBuilder();
        int declength = generateRandom(20,50);
        for(int i = 0; i < declength; i++) {
            int uplow = 0;
            uplow = generateRandom(1,2);
            String character = null;
            switch(uplow) {
                case 1:
                    character = String.valueOf((char)generateRandom(65,90));
                    break;
                case 2:
                    character = String.valueOf((char)generateRandom(97,122));
                    break;
            }
            result.append(character);
        }
        return result.toString();
    }


    private void createTransactions() {
        File file = new File("Transactions.txt");
        try {
            FileWriter fw = new FileWriter(file);
            BufferedWriter bufw = new BufferedWriter(fw);
            for(int i = 0 ; i < 5000000; i++) {
                String content = String.valueOf(i+1) + "," + String.valueOf(generateCustID()) + "," + String.valueOf(generateTransTotal()) + "," + String.valueOf(generateNumItem()) + "," + generateTransDesc();
                bufw.write(content);
                bufw.newLine();
            }
            bufw.close();
            fw.close();
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    // Main function
    public static void main(String[] args) {
        GenerateData createD = new GenerateData();
        createD.createCustomers();
        createD.createTransactions();
    }
}