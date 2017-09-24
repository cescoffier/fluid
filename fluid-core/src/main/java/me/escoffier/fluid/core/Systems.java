package me.escoffier.fluid.core;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Systems {

  public static String getSystemPropertyOrEnvVar(String systemPropertyName, String envVarName, String defaultValue) {
    String answer = System.getProperty(systemPropertyName);
    if (isNotNullOrEmpty(answer)) {
      return answer;
    }

    answer = System.getenv(envVarName);
    if (isNotNullOrEmpty(answer)) {
      return answer;
    }

    return defaultValue;
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName, String defaultValue) {
    return getSystemPropertyOrEnvVar(systemPropertyName, convertSystemPropertyNameToEnvVar(systemPropertyName), defaultValue);
  }

  public static String getSystemPropertyOrEnvVar(String systemPropertyName) {
    return getSystemPropertyOrEnvVar(systemPropertyName, (String) null);
  }

  public static Boolean getSystemPropertyOrEnvVar(String systemPropertyName, Boolean defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, defaultValue.toString());
    return Boolean.parseBoolean(result);
  }

  public static int getSystemPropertyOrEnvVar(String systemPropertyName, int defaultValue) {
    String result = getSystemPropertyOrEnvVar(systemPropertyName, Integer.toString(defaultValue));
    return Integer.parseInt(result);
  }


  public static String convertSystemPropertyNameToEnvVar(String systemPropertyName) {
    return systemPropertyName.toUpperCase().replaceAll("[.-]", "_");
  }

  public static String getEnvVar(String envVarName, String defaultValue) {
    String answer = System.getenv(envVarName);
    return isNotNullOrEmpty(answer) ? answer : defaultValue;
  }

  public static boolean isNotNullOrEmpty(String str) {
    return !isNullOrEmpty(str);
  }

  public static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }

}
