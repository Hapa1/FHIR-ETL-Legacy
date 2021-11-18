using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Microsoft.Data.SqlClient;
using Hl7.Fhir;
using Hl7.Fhir.Serialization;
using System.Data;

namespace FHIRMapper
{
    public static class MapFHIRBulk
    {
        [FunctionName("MapFHIRBulk")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            //string payerClaimUniqueIdentifer = req.Query["PayerClaimUniqueIdentifier"];
            //JArray payerClaimUniqueIdentifiers = req.Query["PayerClaimUniqueIdentifiers"];

            List<JObject> jsonObjectList = new List<JObject>();

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            //payerClaimUniqueIdentifer = payerClaimUniqueIdentifer ?? data?.PayerClaimUniqueIdentifier;
            JArray JPayerClaimUniqueIdentifiers = (JArray)data?.PayerClaimUniqueIdentifiers;
            string[] payerClaimUniqueIdentifiers = JPayerClaimUniqueIdentifiers.ToObject<string[]>();

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = Environment.GetEnvironmentVariable("BuilderDataSource");
            builder.UserID = Environment.GetEnvironmentVariable("BuilderUserID");
            builder.Password = Environment.GetEnvironmentVariable("BuilderPassword");
            builder.InitialCatalog = Environment.GetEnvironmentVariable("BuilderInitialCatalog");

            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {


                //Parallel.ForEach(payerClaimUniqueIdentifiers, payerClaimUniqueIdentifer =>
                //{
                DataSet ds = new DataSet("ClaimsSet");
                List<DataTable> dtList = new List<DataTable>();

                foreach (string payerClaimUniqueIdentifer in payerClaimUniqueIdentifiers) {
                    

                    connection.Open();
                    DataTable dt = new DataTable("Claim" + payerClaimUniqueIdentifer);

                    SqlCommand sql_cmnd = new SqlCommand("selectclaims", connection);
                    sql_cmnd.CommandType = CommandType.StoredProcedure;
                    sql_cmnd.Parameters.AddWithValue("@PayerClaimUniqueIdentifier", payerClaimUniqueIdentifer);

                    SqlDataAdapter da = new SqlDataAdapter();
                    da.SelectCommand = sql_cmnd;
                    da.Fill(dt);
                    
                    ds.Tables.Add(dt);
                    dtList.Add(dt);
                    connection.Close();
                }
                Parallel.ForEach(dtList, dt =>
                {
                    var explanationOfBenefit = new Hl7.Fhir.Model.ExplanationOfBenefit();

                    Parallel.ForEach(dt.AsEnumerable(), row =>
                    {
                        
                        explanationOfBenefit.Item.Add(new Hl7.Fhir.Model.ExplanationOfBenefit.ItemComponent()
                        {
                            Sequence = (int)row["LineNumber"],
                            Category = new Hl7.Fhir.Model.CodeableConcept()
                            {
                                Coding = new List<Hl7.Fhir.Model.Coding>()
                            {
                                new Hl7.Fhir.Model.Coding()
                                {
                                    System = "http://terminology.hl7.org/CodeSystem/ex-benefitcategory"
                                }
                            }
                            },
                            ProductOrService = new Hl7.Fhir.Model.CodeableConcept()
                            {
                                Coding = new List<Hl7.Fhir.Model.Coding>()
                            {
                                new Hl7.Fhir.Model.Coding() { }
                            }
                            },
                            Modifier = new List<Hl7.Fhir.Model.CodeableConcept>()
                            {
                                new Hl7.Fhir.Model.CodeableConcept()
                                {
                                    Coding = new List<Hl7.Fhir.Model.Coding>()
                                    {
                                        new Hl7.Fhir.Model.Coding()
                                        {
                                            Display = "Modifier code-1",
                                            Code = row["ModifierCode1"].ToString()
                                        },
                                        new Hl7.Fhir.Model.Coding()
                                        {
                                            Display = "Modifier code-2",
                                            Code = row["ModifierCode2"].ToString()
                                        },
                                        new Hl7.Fhir.Model.Coding()
                                        {
                                            Display = "Modifier code-3",
                                            Code = row["ModifierCode3"].ToString()
                                        },
                                        new Hl7.Fhir.Model.Coding()
                                        {
                                            Display = "Modifier code-4",
                                            Code = row["ModifierCode4"].ToString()
                                        }
                                    }
                                }
                            }
                        });
                    });
                    /*
                    string sql = "SELECT * FROM Claim WHERE PayerClaimUniqueIdentifier = @PayerClaimUniqueIdentifier";

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {

                        command.Parameters.AddWithValue("@PayerClaimUniqueIdentifier", payerClaimUniqueIdentifiers[0]);
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            var claimTableColumns = new Dictionary<string, int>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                claimTableColumns.Add(reader.GetName(i), i);
                            };

                            while (reader.Read())
                            {
                                explanationOfBenefit.Identifier = new List<Hl7.Fhir.Model.Identifier>()
                                {
                                    new Hl7.Fhir.Model.Identifier()
                                    {
                                        Type = new Hl7.Fhir.Model.CodeableConcept()
                                        {
                                            Text = "Claim unique identifier",
                                            Coding = new List<Hl7.Fhir.Model.Coding>()
                                            {
                                                new Hl7.Fhir.Model.Coding()
                                                {
                                                    System = "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBIdentifierType",
                                                    Code = "uc",
                                                    Display = "Unique Claim ID"
                                                }
                                            }
                                        },
                                        Value = reader.GetString(claimTableColumns["PayerClaimUniqueIdentifier"])
                                    }
                                };
                                explanationOfBenefit.Status = reader.GetString(claimTableColumns["ClaimProcessingStatusCode"]) == "cancelled" ? Hl7.Fhir.Model.ExplanationOfBenefit.ExplanationOfBenefitStatus.Cancelled : Hl7.Fhir.Model.ExplanationOfBenefit.ExplanationOfBenefitStatus.Active;

                                // TODO: explanationOfBenefit.Type is currently hardcoded
                                explanationOfBenefit.Type = new Hl7.Fhir.Model.CodeableConcept()
                                {
                                    Coding = new List<Hl7.Fhir.Model.Coding>()
                                {
                                    new Hl7.Fhir.Model.Coding()
                                    {
                                        System = "https://www.hl7.org/fhir/codesystem-claim-type.html",
                                        Code = "Institutional",
                                        Display = "Institutional"
                                    }
                                },
                                    Text = "Hospital, clinic and typically inpatient claims.",
                                };

                                explanationOfBenefit.Use = Hl7.Fhir.Model.Use.Claim;

                                // TODO: Add referenceable patient 
                                explanationOfBenefit.Patient = new Hl7.Fhir.Model.ResourceReference()
                                {
                                    Reference = "Patient/",
                                    Display = reader.GetString(claimTableColumns["PatientAccountNumber"])
                                };

                                explanationOfBenefit.Created = reader.GetDateTime(claimTableColumns["ClaimReceivedDateMedical"]).ToString("yyyy-MM-dd");

                                // TODO: Add referenceable organization 
                                explanationOfBenefit.Insurer = new Hl7.Fhir.Model.ResourceReference()
                                {
                                    Reference = "Organization/"
                                };

                                // TODO: Add referenceable organization 
                                explanationOfBenefit.Provider = new Hl7.Fhir.Model.ResourceReference()
                                {
                                    Reference = "Organization/"
                                };

                                // TODO: Add rest of totals
                                explanationOfBenefit.Total = new List<Hl7.Fhir.Model.ExplanationOfBenefit.TotalComponent>()
                                {
                                    new Hl7.Fhir.Model.ExplanationOfBenefit.TotalComponent()
                                    {
                                        Category = new Hl7.Fhir.Model.CodeableConcept()
                                        {
                                            Coding = new List<Hl7.Fhir.Model.Coding>()
                                            {
                                                new Hl7.Fhir.Model.Coding()
                                                {
                                                    Code = "submitted",
                                                    System = "http://terminology.hl7.org/CodeSystem/adjudication"
                                                }
                                            }
                                        },
                                        Amount = new Hl7.Fhir.Model.Money()
                                        {
                                            Value = reader.GetDecimal(claimTableColumns["ClaimTotalSubmittedAmount"])
                                        }
                                    }
                                };

                                // TODO: Finish Payment
                                explanationOfBenefit.Payment = new Hl7.Fhir.Model.ExplanationOfBenefit.PaymentComponent()
                                {

                                };

                                // TODO: Finish Diagnosis
                                explanationOfBenefit.Diagnosis = new List<Hl7.Fhir.Model.ExplanationOfBenefit.DiagnosisComponent>()
                                {

                                };

                                // TODO: Finish Insurance
                                explanationOfBenefit.Insurance = new List<Hl7.Fhir.Model.ExplanationOfBenefit.InsuranceComponent>()
                                {
                                     new Hl7.Fhir.Model.ExplanationOfBenefit.InsuranceComponent()
                                     {
                                         Focal = true,
                                         // TODO: Add Coverage reference
                                         Coverage = new Hl7.Fhir.Model.ResourceReference()
                                         {
                                             Reference = "Coverage/"
                                         }
                                     }
                                };

                                // TODO: Finish Outcome
                                explanationOfBenefit.Outcome = Hl7.Fhir.Model.ClaimProcessingCodes.Complete;

                                // TODO: Finish SupportingInfo
                                explanationOfBenefit.SupportingInfo = new List<Hl7.Fhir.Model.ExplanationOfBenefit.SupportingInformationComponent>()
                                {

                                };

                                // TODO: Finish procedure
                                explanationOfBenefit.Procedure = new List<Hl7.Fhir.Model.ExplanationOfBenefit.ProcedureComponent>()
                                {

                                };

                                // TODO: Finish BillablePeriod
                                explanationOfBenefit.BillablePeriod = new Hl7.Fhir.Model.Period()
                                {

                                };
                            }
                        }
                    }
                    */
                    JObject jsonObject = new JObject();
                    jsonObject.Add(new JProperty("Output", explanationOfBenefit.ToJObject()));
                    jsonObjectList.Add(jsonObject);
                });             
            }
            return new OkObjectResult(jsonObjectList);
        }
    }
}
